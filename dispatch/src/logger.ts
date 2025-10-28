import pino from "pino";

// Create the base pino logger
const baseLogger = pino({
    level: process.env.LOG_LEVEL || "info",
    transport:
        process.env.NODE_ENV === "development"
            ? {
                target: "pino-pretty",
                options: { colorize: true, translateTime: "SYS:standard" },
            }
            : undefined,
});

// Extend logger with console-style fallback methods
export const logger = {
    ...baseLogger,
    info: (message: string, ...args: any[]) => {
        baseLogger.info(message, ...args);
        if (process.env.NODE_ENV === "development") {
            console.log(`[INFO] ${message}`, ...args);
        }
    },
    error: (message: string, ...args: any[]) => {
        baseLogger.error(message, ...args);
        if (process.env.NODE_ENV === "development") {
            console.error(`[ERROR] ${message}`, ...args);
        }
    },
    warn: (message: string, ...args: any[]) => {
        baseLogger.warn(message, ...args);
        if (process.env.NODE_ENV === "development") {
            console.warn(`[WARN] ${message}`, ...args);
        }
    },
    debug: (message: string, ...args: any[]) => {
        baseLogger.debug(message, ...args);
        if (process.env.NODE_ENV === "development") {
            console.debug(`[DEBUG] ${message}`, ...args);
        }
    },
};

export default logger;
