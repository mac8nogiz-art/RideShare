import pino from "pino";

export const logger = pino({
  name: "dispatch-service",
  level: process.env.NODE_ENV === "production" ? "info" : "debug",
  transport:
    process.env.NODE_ENV !== "production"
      ? {
          target: "pino-pretty",
          options: { colorize: true, translateTime: "HH:MM:ss Z", ignore: "pid,hostname" },
        }
      : undefined,
});
