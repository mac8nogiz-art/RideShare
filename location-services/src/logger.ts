import pino from "pino";

export const logger = pino({
    level: process.env.LOG_LEVEL || "info",
    transport: process.env.NODE_ENV === "development"
        ? undefined // disables fancy transports
        : undefined,
});

export default logger;
