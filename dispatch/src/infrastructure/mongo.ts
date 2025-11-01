import mongoose from "mongoose";
import { logger } from "../logger";

let isConnected = false;

export async function connectMongo(): Promise<typeof mongoose> {
    if (isConnected) {
        logger.info("MongoDB already connected - reusing connection");
        return mongoose;
    }

    const uri = process.env.MONGO_URI;
    if (!uri) {
        throw new Error("MONGO_URI is not defined in environment variables");
    }

    try {
        await mongoose.connect(uri, {

            maxPoolSize: 10,
            serverSelectionTimeoutMS: 5000,
            socketTimeoutMS: 45000,
        });

        isConnected = true;

        const dbName = mongoose.connection.db?.databaseName || 'unknown';
        logger.info(`✅Connected to MongoDB database: ${dbName}`);

        // Handle connection events
        mongoose.connection.on('error', (err) => {
            logger.error('MongoDB connection error:', err);
            isConnected = false;
        });

        mongoose.connection.on('disconnected', () => {
            logger.warn('MongoDB disconnected');
            isConnected = false;
        });

        mongoose.connection.on('reconnected', () => {
            logger.info('MongoDB reconnected');
            isConnected = true;
        });

        return mongoose;
    } catch (error: any) {
        logger.error("Failed to connect to MongoDB:", error);
        isConnected = false;
        throw error;
    }
}

export function getMongoose(): typeof mongoose {
    if (!isConnected || !mongoose.connection.readyState) {
        throw new Error("MongoDB not connected. Call connectMongo first.");
    }
    return mongoose;
}

export function getMongoDB() {
    if (!isConnected || !mongoose.connection.db) {
        throw new Error("MongoDB not connected. Call connectMongo first.");
    }
    return mongoose.connection.db;
}

export function getMongoClient() {
    if (!isConnected || !mongoose.connection.getClient()) {
        throw new Error("MongoDB not connected. Call connectMongo first.");
    }
    return mongoose.connection.getClient();
}

export async function closeMongo(): Promise<void> {
    if (isConnected) {
        await mongoose.connection.close();
        isConnected = false;
        logger.info("MongoDB connection closed");
    }
}

// Check if connected
export function isMongoConnected(): boolean {
    return isConnected && mongoose.connection.readyState === 1;
}

// Export mongoose instance directly
export { mongoose };
export default mongoose;