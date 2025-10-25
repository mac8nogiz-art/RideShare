import { Kafka, Producer } from 'kafkajs';

// -------------------------------
// In-Memory Redis Simulator
// -------------------------------
class InMemoryRedis {
    private hashStore: Map<string, any> = new Map();
    private setStore: Map<string, Set<string>> = new Map();
    private geoStore: Map<string, Array<{ id: string; lon: number; lat: number }>> = new Map();

    async hset(key: string, obj: Record<string, any>) {
        this.hashStore.set(key, { ...(this.hashStore.get(key) || {}), ...obj });
    }

    async hgetall(key: string) {
        return this.hashStore.get(key) || {};
    }

    async sadd(key: string, ...values: string[]) {
        if (!this.setStore.has(key)) this.setStore.set(key, new Set());
        const set = this.setStore.get(key)!;
        values.forEach(v => set.add(v));
    }

    async smembers(key: string) {
        return Array.from(this.setStore.get(key) || []);
    }

    async geoadd(key: string, lon: number, lat: number, id: string) {
        if (!this.geoStore.has(key)) this.geoStore.set(key, []);
        this.geoStore.get(key)!.push({ id, lon, lat });
    }

    async keys(pattern: string) {
        const allKeys = [...this.hashStore.keys(), ...this.setStore.keys(), ...this.geoStore.keys()];
        const regex = new RegExp(pattern.replace('*', '.*'));
        return allKeys.filter(k => regex.test(k));
    }

    async del(...keys: string[]) {
        keys.forEach(k => {
            this.hashStore.delete(k);
            this.setStore.delete(k);
            this.geoStore.delete(k);
        });
    }

    async ping() {
        return 'PONG';
    }
}

// -------------------------------
// Kafka Configuration
// -------------------------------
const kafka = new Kafka({
    clientId: 'driver-generator',
    brokers: ['172.105.61.99:9093'],
});
const producer: Producer = kafka.producer();

// -------------------------------
// Configuration
// -------------------------------
const CONFIG = {
    NUM_DRIVERS: 8,
    CITY_CENTER: { lat: 47.61250425448169, lng: -52.742545930897144 },
    RADIUS_KM: 5,
};

const ZONE_ID = '68062fe1d17296e5867ddae1';

// -------------------------------
// Helpers
// -------------------------------
function generateLocation(center: { lat: number; lng: number }, radiusKm: number) {
    const radiusInDegrees = radiusKm / 111.32;
    const u = Math.random();
    const v = Math.random();
    const w = radiusInDegrees * Math.sqrt(u);
    const t = 2 * Math.PI * v;
    const x = w * Math.cos(t);
    const y = w * Math.sin(t);
    return { latitude: center.lat + y, longitude: center.lng + x };
}

function generateObjectId() {
    const timestamp = Math.floor(Date.now() / 1000).toString(16).padStart(8, '0');
    const randomHex = Array.from({ length: 16 }, () => Math.floor(Math.random() * 16).toString(16)).join('');
    return timestamp + randomHex;
}

function generateApprovedZones(driverIndex: number) {
    return driverIndex < 3 ? [] : [ZONE_ID];
}

function generateDriver(id: number) {
    const location = generateLocation(CONFIG.CITY_CENTER, CONFIG.RADIUS_KM);
    const isOnline = Math.random() > 0.2;
    const isBusy = isOnline ? Math.random() > 0.7 : false;
    const driverId = generateObjectId();

    return {
        _id: driverId,
        fcmToken: `token_${String(id).padStart(10, '0')}`,
        vehicleInfo: { isApproved: Math.random() > 0.1 },
        iAmOnline: isOnline,
        iAmBusy: isBusy,
        location: { type: 'Point', coordinates: [location.longitude, location.latitude] },
        fullName: `Driver ${id}`,
        approved_zones: generateApprovedZones(id - 1),
    };
}

// -------------------------------
// Generate & Upload Drivers in Memory
// -------------------------------
async function generateAndUploadDrivers(mockRedis: InMemoryRedis) {
    console.log('🚀 Generating drivers in memory...');

    const drivers = [];
    for (let i = 1; i <= CONFIG.NUM_DRIVERS; i++) {
        const driver = generateDriver(i);
        drivers.push(driver);

        // Simulate Redis storage
        const driverKey = `driver:${driver._id}`;
        await mockRedis.hset(driverKey, driver);
        if (driver.iAmOnline) await mockRedis.sadd('drivers:online', driver._id);
        if (driver.iAmBusy) await mockRedis.sadd('drivers:busy', driver._id);
        if (driver.vehicleInfo.isApproved) await mockRedis.sadd('drivers:approved', driver._id);
        if (driver.approved_zones.length && driver.iAmOnline && !driver.iAmBusy && driver.vehicleInfo.isApproved) {
            await mockRedis.sadd(`zone:${ZONE_ID}:available_drivers`, driver._id);
        }

        // Send Kafka event
        await producer.send({
            topic: 'driver.events',
            messages: [{ key: driver._id, value: JSON.stringify({ type: 'driver.created', payload: driver }) }],
        });
        console.log(`📤 Driver created: ${driver.fullName}`);
    }

    return drivers;
}

// -------------------------------
// Simulate Booking & Driver Response
// -------------------------------
async function simulateDriverResponses(mockRedis: InMemoryRedis, bookingId: string) {
    console.log(`\n🔔 Simulating responses for booking ${bookingId}...`);
    const availableDrivers = await mockRedis.smembers(`zone:${ZONE_ID}:available_drivers`);
    if (!availableDrivers.length) return console.log('❌ No available drivers.');

    let assignedDriver: string | null = null;

    for (const driverId of availableDrivers) {
        await new Promise(res => setTimeout(res, Math.random() * 1000));
        const accepted = Math.random() > 0.5;
        if (accepted && !assignedDriver) assignedDriver = driverId;

        await producer.send({
            topic: 'driver.events',
            messages: [
                {
                    key: driverId,
                    value: JSON.stringify({
                        type: 'driver.offer.responded',
                        payload: { driverId, bookingId, status: accepted ? 'accepted' : 'rejected' },
                    }),
                },
            ],
        });

        console.log(`Driver ${driverId} ${accepted ? '✅ accepted' : '❌ rejected'} the booking`);
    }

    if (assignedDriver) {
        console.log(`\n🏆 Booking ${bookingId} assigned to driver ${assignedDriver}`);
        await mockRedis.hset(`booking:${bookingId}`, { assignedDriver });
    } else {
        console.log(`\n⚠️ No driver accepted booking ${bookingId}`);
    }
}

// -------------------------------
// Main Execution
// -------------------------------
(async () => {
    const mockRedis = new InMemoryRedis();
    await producer.connect();
    console.log('✅ Kafka producer connected');

    const drivers = await generateAndUploadDrivers(mockRedis);

    const bookingId = 'booking_' + Date.now();
    await simulateDriverResponses(mockRedis, bookingId);

    await producer.disconnect();
    console.log('\n✅ Simulation complete!');
})();
