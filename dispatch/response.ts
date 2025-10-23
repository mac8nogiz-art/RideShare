import Redis from 'ioredis';

const redis = new Redis('redis://localhost:6379');

async function diagnoseRedis() {
    console.log('🔍 Redis Diagnostic Tool\n');

    try {
        // Check location keys
        console.log('1️⃣  Checking driver location keys...');
        const locationKeys = await redis.keys('driver:*:location');
        console.log(`   Found ${locationKeys.length} location keys`);

        if (locationKeys.length > 0) {
            console.log('\n   Sample locations:');
            for (let i = 0; i < Math.min(3, locationKeys.length); i++) {
                const key = locationKeys[i];
                const data = await redis.hgetall(key);
                console.log(`   - ${key}:`, data);
            }
        }

        // Check profile keys
        console.log('\n2️⃣  Checking driver profile keys...');
        const profileKeys = await redis.keys('driver:*:profile');
        console.log(`   Found ${profileKeys.length} profile keys`);

        if (profileKeys.length > 0) {
            console.log('\n   Sample profiles:');
            for (let i = 0; i < Math.min(3, profileKeys.length); i++) {
                const key = profileKeys[i];
                const data = await redis.hgetall(key);
                console.log(`   - ${key}:`, data);
            }
        }

        // Check approved zones
        console.log('\n3️⃣  Checking approved zones...');
        const zoneKeys = await redis.keys('driver:*:approved_zones');
        console.log(`   Found ${zoneKeys.length} zone keys`);

        if (zoneKeys.length > 0) {
            console.log('\n   Sample approved zones:');
            for (let i = 0; i < Math.min(3, zoneKeys.length); i++) {
                const key = zoneKeys[i];
                const zones = await redis.smembers(key);
                console.log(`   - ${key}:`, zones);
            }
        }

        // Check geospatial index
        console.log('\n4️⃣  Checking geospatial index...');
        const geoCount = await redis.zcard('drivers:locations');
        console.log(`   Entries in drivers:locations: ${geoCount}`);

        if (geoCount > 0) {
            const geoMembers = await redis.zrange('drivers:locations', 0, 4, 'WITHSCORES');
            console.log('\n   Sample geospatial entries:');
            for (let i = 0; i < geoMembers.length; i += 2) {
                console.log(`   - ${geoMembers[i]}: ${geoMembers[i + 1]}`);
            }

            // Test GEOSEARCH
            console.log('\n   Testing GEOSEARCH (St. John\'s Airport area)...');
            const searchResult = await redis.geosearch(
                'drivers:locations',
                'FROMLONLAT', -52.742546, 47.612504,
                'BYRADIUS', 15, 'km',
                'WITHDIST',
                'COUNT', 5
            );
            console.log(`   Found ${searchResult.length / 2} drivers within 15km:`);
            for (let i = 0; i < searchResult.length; i += 2) {
                console.log(`   - ${searchResult[i]}: ${searchResult[i + 1]}km`);
            }
        }

        // Check active zones
        console.log('\n5️⃣  Checking active zones...');
        const zonesData = await redis.get('zones:active');
        if (zonesData) {
            const zones = JSON.parse(zonesData);
            console.log(`   Active zones: ${zones.length}`);
            zones.forEach((zone: any) => {
                console.log(`   - ${zone.name} (${zone._id})`);
            });
        } else {
            console.log('   ⚠️  No active zones found');
        }

        console.log('\n✅ Diagnostic complete');

    } catch (error) {
        console.error('❌ Diagnostic failed:', error);
    } finally {
        await redis.quit();
    }
}

diagnoseRedis();