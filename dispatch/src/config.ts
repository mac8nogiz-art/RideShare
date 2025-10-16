// zod based schema validation for -------->env variables
import { z } from 'zod'

const EnvSchema = z.object({

  PORT: z.string().default('3000'),
  KAFKA_BROKER: z.string(),
    REDIS_HOST: z.string(),
  SERVICE_NAME: z.string().default('dispatch-service'),
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
    DRIVER_LOCATION_TOPIC: z.string().default("driver.location"),
    RIDE_REQUEST_TOPIC: z.string().default("ride.request"),


})

const env = EnvSchema.parse(process.env)

export const config = {

  port: Number(env.PORT),
  kafkaBrokers: env.KAFKA_BROKER.split(','),
  redisUrl: env.REDIS_HOST,
  serviceName: env.SERVICE_NAME,
  nodeEnv: env.NODE_ENV,
    driverEnv: env.DRIVER_LOCATION_TOPIC,
    rideRequestTopic: env.RIDE_REQUEST_TOPIC,
}
