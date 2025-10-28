// scripts/fix-polygons.ts
import { connectMongo, getMongoDB } from "../dispatch/src/infrastructure/mongo";
import logger from "../dispatch/src/logger";

async function fixPolygons() {
    try {
        await connectMongo();
        const db = getMongoDB();

        const zones = await db.collection('geoareas').find({}).toArray();

        console.log(`🔍 Checking ${zones.length} zones...`);

        let fixedCount = 0;

        for (const zone of zones) {
            if (zone.location && zone.location.coordinates && zone.location.coordinates[0]) {
                const coordinates = zone.location.coordinates[0];
                console.log(`\n--- Zone: ${zone.name} (${zone._id}) ---`);
                console.log(`Original points: ${coordinates.length}`);

                // Check if polygon is closed
                const first = coordinates[0];
                const last = coordinates[coordinates.length - 1];
                const isClosed = first[0] === last[0] && first[1] === last[1];

                console.log(`First point: [${first[0]}, ${first[1]}]`);
                console.log(`Last point:  [${last[0]}, ${last[1]}]`);
                console.log(`Closed ring: ${isClosed}`);

                if (!isClosed) {
                    console.log('❌ Polygon is NOT closed - fixing...');

                    // Close the polygon by adding the first point at the end
                    const fixedCoordinates = [...coordinates, first];

                    // Update the zone in database
                    await db.collection('geoareas').updateOne(
                        { _id: zone._id },
                        {
                            $set: {
                                'location.coordinates': [fixedCoordinates]
                            }
                        }
                    );

                    console.log(`✅ Fixed - Now has ${fixedCoordinates.length} points`);
                    fixedCount++;
                } else {
                    console.log('✅ Polygon is already closed');
                }
            }
        }

        console.log(`\n🎉 Fixed ${fixedCount} polygons`);
        process.exit(0);

    } catch (error: any) {
        console.error('❌ Error fixing polygons:', error);
        process.exit(1);
    }
}

fixPolygons();