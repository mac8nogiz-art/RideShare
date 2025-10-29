// // master-driver-simulator.ts
// import Redis from 'ioredis';
// import fs from 'fs';
// import path from 'path';
// import { Kafka, Producer } from 'kafkajs';
//
// // -------------------------------
// // Redis Configuration
// // -------------------------------
// const redis = new Redis({
//     host: 'redis-13830.fcrce180.us-east-1-1.ec2.redns.redis-cloud.com',
//     port: 13830,
//     username: 'default',
//     password: 'bWrQDBwpRAaOPrtPYvgjfrFbo1rdFOB0',
// });
//
// // -------------------------------
// // Kafka Configuration
// // -------------------------------
// const kafka = new Kafka({
//     clientId: 'driver-generator',
//     brokers: ['172.105.61.99:9093'], // replace with your Kafka broker
// });
//
// const producer: Producer = kafka.producer();
//
// // -------------------------------
// // Configuration
// // -------------------------------
// const CONFIG = {
//     NUM_DRIVERS: 8,
//     CITY_CENTER: { lat: 47.61250425448169, lng: -52.742545930897144 },
//     RADIUS_KM: 5,
//     OUTPUT_DIR: './test_drivers',
// };
//
// const ZONE_ID = '68062fe1d17296e5867ddae1';
//
// // -------------------------------
// // Helper Functions
// // -------------------------------
// function generateLocation(center: { lat: number; lng: number }, radiusKm: number) {
//     const radiusInDegrees = radiusKm / 111.32;
//     const u = Math.random();
//     const v = Math.random();
//     const w = radiusInDegrees * Math.sqrt(u);
//     const t = 2 * Math.PI * v;
//     const x = w * Math.cos(t);
//     const y = w * Math.sin(t);
//
//     return {
//         latitude: center.lat + y,
//         longitude: center.lng + x,
//     };
// }
//
// function generateObjectId() {
//     const timestamp = Math.floor(Date.now() / 1000).toString(16).padStart(8, '0');
//     const randomHex = Array.from({ length: 16 }, () => Math.floor(Math.random() * 16).toString(16)).join('');
//     return timestamp + randomHex;
// }
//
// function generateApprovedZones(driverIndex: number) {
//     return driverIndex < 3 ? [] : [ZONE_ID];
// }
//
// function generateDriver(id: number) {
//     const location = generateLocation(CONFIG.CITY_CENTER, CONFIG.RADIUS_KM);
//     const isOnline = Math.random() > 0.2;
//     const isBusy = isOnline ? Math.random() > 0.7 : false;
//     const driverId = generateObjectId();
//
//     return {
//         _id: driverId,
//         fcmToken: `dE7W94fDP0VRvBaa_iAVEe:APA91bEsGKyPxQKIN8GwJg3h${String(id).padStart(10, '0')}`,
//         vehicleInfo: { isApproved: Math.random() > 0.1 },
//         iAmOnline: isOnline,
//         location: { type: 'Point', coordinates: [location.longitude, location.latitude] },
//         missedBookingCount: Math.floor(Math.random() * 3),
//         heading: Math.floor(Math.random() * 360) - 1,
//         missedBookingAt: null,
//         stopFutureRide: false,
//         iAmBusy: isBusy,
//         socket_id: driverId,
//         isDriverUnderPool: false,
//         fullName: `Driver ${id}`,
//         avatar: `https://s3.ca-central-1.wasabisys.com/webin10/websites/65f352b0d0b96c21405031be/drivers/${driverId}/h5z4pyca-rn_image_picker_lib_temp_${driverId}.webp`,
//         phone: `92${String(id).padStart(8, '0')}`,
//         approved_zones: generateApprovedZones(id - 1),
//     };
// }
//
// // -------------------------------
// // Generate & Upload Drivers
// // -------------------------------
// async function generateAndUploadDrivers() {
//     console.log('🚀 Generating drivers and uploading to Redis & Kafka...\n');
//
//     await producer.connect();
//     console.log('✅ Kafka producer connected!\n');
//
//     try {
//         await redis.ping();
//         console.log('✅ Redis connected!\n');
//
//         // Clean previous data
//         const existingKeys = await redis.keys('driver:*');
//         if (existingKeys.length > 0) await redis.del(...existingKeys);
//         await redis.del('geo:drivers', 'drivers:online', 'drivers:busy', 'drivers:approved');
//         const zoneKeys = await redis.keys(`zone:${ZONE_ID}:*`);
//         if (zoneKeys.length > 0) await redis.del(...zoneKeys);
//
//         if (!fs.existsSync(CONFIG.OUTPUT_DIR)) fs.mkdirSync(CONFIG.OUTPUT_DIR, { recursive: true });
//
//         const drivers = [];
//         const pipeline = redis.pipeline();
//
//         for (let i = 1; i <= CONFIG.NUM_DRIVERS; i++) {
//             const driver = generateDriver(i);
//             drivers.push(driver);
//
//             // Save JSON locally
//             fs.writeFileSync(path.join(CONFIG.OUTPUT_DIR, `driver_${i}_${driver._id}.json`), JSON.stringify(driver, null, 2));
//
//             // Upload to Redis
//             const driverKey = `driver:${driver._id}`;
//             pipeline.hset(driverKey, {
//                 _id: driver._id,
//                 fcmToken: driver.fcmToken,
//                 vehicleInfo: JSON.stringify(driver.vehicleInfo),
//                 iAmOnline: driver.iAmOnline ? '1' : '0',
//                 location: JSON.stringify(driver.location),
//                 missedBookingCount: driver.missedBookingCount.toString(),
//                 heading: driver.heading.toString(),
//                 missedBookingAt: driver.missedBookingAt || '',
//                 stopFutureRide: driver.stopFutureRide ? '1' : '0',
//                 iAmBusy: driver.iAmBusy ? '1' : '0',
//                 socket_id: driver.socket_id,
//                 isDriverUnderPool: driver.isDriverUnderPool ? '1' : '0',
//                 fullName: driver.fullName,
//                 avatar: driver.avatar,
//                 phone: driver.phone,
//                 approved_zones: JSON.stringify(driver.approved_zones),
//             });
//             pipeline.expire(driverKey, 86400);
//
//             // Geospatial & status
//             if (driver.iAmOnline) pipeline.geoadd('geo:drivers', driver.location.coordinates[0], driver.location.coordinates[1], driver._id);
//             if (driver.iAmOnline) pipeline.sadd('drivers:online', driver._id);
//             if (driver.iAmBusy) pipeline.sadd('drivers:busy', driver._id);
//             if (driver.vehicleInfo.isApproved) pipeline.sadd('drivers:approved', driver._id);
//             if (driver.approved_zones.length > 0 && driver.iAmOnline && !driver.iAmBusy && driver.vehicleInfo.isApproved) {
//                 pipeline.sadd(`zone:${ZONE_ID}:available_drivers`, driver._id);
//             }
//
//             // Kafka event: driver.created
//             await producer.send({
//                 topic: 'driver.events',
//                 messages: [
//                     { key: driver._id, value: JSON.stringify({ type: 'driver.created', payload: driver }) },
//                 ],
//             });
//             console.log(`📤 Kafka event sent for ${driver.fullName}`);
//         }
//
//         await pipeline.exec();
//         console.log('\n✅ Drivers uploaded to Redis and Kafka!\n');
//
//         return drivers;
//     } catch (error) {
//         console.error('❌ Error generating drivers:', error);
//         throw error;
//     }
// }
//
// // -------------------------------
// // Simulate Driver Responses for a Booking
// // -------------------------------
// async function simulateDriverResponses(bookingId: string) {
//     console.log(`\n🔔 Simulating driver responses for booking ${bookingId}...`);
//
//     const availableDrivers = await redis.smembers(`zone:${ZONE_ID}:available_drivers`);
//     if (!availableDrivers.length) {
//         console.log('❌ No available drivers for this booking.');
//         return;
//     }
//
//     let assignedDriver: string | null = null;
//
//     for (const driverId of availableDrivers) {
//         await new Promise(res => setTimeout(res, Math.random() * 2000));
//         const accepted = Math.random() > 0.5;
//
//         const offerKey = `offer:${bookingId}:${driverId}`;
//         await redis.hset(offerKey, 'status', accepted ? 'accepted' : 'rejected');
//
//         await producer.send({
//             topic: 'driver.events',
//             messages: [
//                 {
//                     key: driverId,
//                     value: JSON.stringify({
//                         type: 'driver.offer.responded',
//                         payload: { driverId, bookingId, status: accepted ? 'accepted' : 'rejected', respondedAt: new Date().toISOString() },
//                     }),
//                 },
//             ],
//         });
//
//         console.log(`Driver ${driverId} ${accepted ? '✅ accepted' : '❌ rejected'} the ride`);
//
//         if (accepted) {
//             assignedDriver = driverId;
//             break;
//         }
//     }
//
//     if (assignedDriver) {
//         console.log(`\n🏆 Booking ${bookingId} assigned to driver ${assignedDriver}`);
//         await redis.hset(`booking:${bookingId}`, 'assignedDriver', assignedDriver);
//     } else {
//         console.log(`\n⚠️ No driver accepted booking ${bookingId}`);
//     }
// }
//
// // -------------------------------
// // Execute
// // -------------------------------
// if (require.main === module) {
//     generateAndUploadDrivers()
//         .then(async (drivers) => {
//             const bookingId = 'booking_' + Date.now();
//             console.log(`\n🎯 Simulating booking: ${bookingId}`);
//
//             const availableDrivers = await redis.smembers(`zone:${ZONE_ID}:available_drivers`);
//             if (availableDrivers.length) {
//                 await redis.sadd(`job:${bookingId}:pending_drivers`, ...availableDrivers);
//             }
//
//             await simulateDriverResponses(bookingId);
//
//             await producer.disconnect();
//             redis.disconnect();
//             console.log('\n✅ Simulation completed.');
//             process.exit(0);
//         })
//         .catch((error) => {
//             console.error('Fatal error:', error);
//             process.exit(1);
//         });
// }
//
// export { generateAndUploadDrivers, simulateDriverResponses, redis, ZONE_ID };
