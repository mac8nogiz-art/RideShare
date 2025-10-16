import { Elysia } from "elysia";
import { connectKafka } from "./infrastructure/kafka";
import { driverWS } from "./ws/driver.ws";
import { logger } from "./logger";
import { config } from "./config";

async function start() {
    await connectKafka();

    const app = new Elysia().use(driverWS).listen(config.port);
    logger.info(`📍 Location Service running on port ${config.port}`);
}

start();
