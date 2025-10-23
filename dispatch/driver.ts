import { redis } from './src/infrastructure/redis';
import logger from './src/logger';

// Example zone polygon (St. John's Airport)
const zone = {
    _id: "68062fe1d17296e5867ddae1",
    name: "St. John's International Airport",
    geoPoint: { type: "Point", coordinates: [-52.742545930897144, 47.61250425448169] },
    location: {
        type: "Polygon",
        coordinates: [[
            [-52.77172627738214,47.62434004791682],
            [-52.75224271109796,47.625063154256985],
            [-52.75254311850763,47.626075486328354],
            [-52.75116982749201,47.62624902700056],
            [-52.750225689918764,47.625497013259675],
            [-52.73705067923761,47.62572840325537],
            [-52.73705067923761,47.62488960964761],
            [-52.739196446449526,47.62474498869967],
            [-52.738209393532046,47.61557520376109],
            [-52.73305955222345,47.61164067051498],
            [-52.734261181862124,47.61085951472438],
            [-52.73765149405695,47.6119589158253],
            [-52.73803773215509,47.611467081347605],
            [-52.74151387503839,47.611119901284354],
            [-52.74258675864435,47.612682193419026],
            [-52.74426045706964,47.613159551151085],
            [-52.74565520575739,47.612870243954866],
            [-52.74487200072504,47.61451204106197],
            [-52.74490418723322,47.61520635384489],
            [-52.742490199119814,47.61592235424667],
            [-52.74239363959528,47.61627673465673],
            [-52.74490418723322,47.615517345122775],
            [-52.744936373741396,47.61732539731092],
            [-52.74554791739679,47.61728200479063],
            [-52.74607363036371,47.61905383674879],
            [-52.7522748923556,47.618280045314926],
            [-52.756888291861216,47.616717920419255],
            [-52.76459159615199,47.62186697029624],
            [-52.76575031044642,47.622503334508416],
            [-52.77201595070521,47.6227347377559],
            [-52.7727884269015,47.623544641055716],
            [-52.77172627738214,47.62434004791682]
        ]]
    },
    status: true
};

// Simple helper to generate random coordinates inside bounding box of polygon
function randomPointInBBox(polygon: number[][]) {
    const lons = polygon.map(p => p[0]);
    const lats = polygon.map(p => p[1]);
    const minLon = Math.min(...lons);
    const maxLon = Math.max(...lons);
    const minLat = Math.min(...lats);
    const maxLat = Math.max(...lats);

    return [
        minLon + Math.random() * (maxLon - minLon),
        minLat + Math.random() * (maxLat - minLat)
    ];
}

async function main() {
    logger.info("🚀 Seeding polygon test drivers...");

    // --- Step 1: Seed Zone ---
    await redis.set('zones:active', JSON.stringify([zone]));
    logger.info(`✅ Zone added: ${zone.name}`);

    // --- Step 2: Generate 10 drivers inside the polygon ---
    const driversInside = Array.from({ length: 10 }, (_, i) => {
        const [lng, lat] = randomPointInBBox(zone.location.coordinates[0]);
        return {
            id: `driver-in-${i+1}`,
            lat,
            lng,
            score: Math.floor(Math.random() * 50) + 50, // 50-100
            isBusy: Math.random() < 0.2,
            approvedZones: [zone._id]
        };
    });

    // --- Step 3: Generate 5 drivers outside the polygon ---
    const driversOutside = Array.from({ length: 5 }, (_, i) => {
        // arbitrary point outside the polygon bounding box
        return {
            id: `driver-out-${i+1}`,
            lat: zone.geoPoint.coordinates[1] + (Math.random() > 0.5 ? 0.02 : -0.02),
            lng: zone.geoPoint.coordinates[0] + (Math.random() > 0.5 ? 0.02 : -0.02),
            score: Math.floor(Math.random() * 50) + 50,
            isBusy: false,
            approvedZones: [] // no approval
        };
    });

    const allDrivers = [...driversInside, ...driversOutside];

    const pipeline = redis.pipeline();
    for (const driver of allDrivers) {
        pipeline.hset(`driver:${driver.id}:location`, {
            lat: driver.lat.toString(),
            lng: driver.lng.toString(),
            ts: Date.now().toString()
        });
        pipeline.hset(`driver:${driver.id}:profile`, {
            score: driver.score.toString(),
            isBusy: driver.isBusy.toString(),
            approvedZones: JSON.stringify(driver.approvedZones),
            approvedDate: new Date().toISOString()
        });
        pipeline.del(`driver:${driver.id}:approved_zones`);
        driver.approvedZones.forEach(zoneId => pipeline.sadd(`driver:${driver.id}:approved_zones`, zoneId));
        pipeline.geoadd('drivers:locations', driver.lng, driver.lat, driver.id);
    }

    await pipeline.exec();
    logger.info(`✅ Drivers seeded: ${allDrivers.length}`);

    logger.info("🎯 Polygon driver test data ready!");
    process.exit(0);
}

main().catch(err => {
    logger.error("❌ Error seeding polygon drivers:", err);
    process.exit(1);
});
