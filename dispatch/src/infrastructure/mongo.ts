import { MongoClient, Db } from "mongodb";
import logger from "../logger";

let client: MongoClient;
let db: Db;


export async function connectMongo(): Promise<Db> {
    if (db) return db;

    const uri = process.env.MONGO_URI;
    if (!uri) throw new Error("MONGO_URI is not defined in environment variables");

    try {
        client = new MongoClient(uri);
        await client.connect();

        const dbName = uri.split("/").pop()?.split("?")[0];
        if (!dbName) throw new Error("Cannot parse database name from MONGO_URI");

        db = client.db(dbName);
        logger.info(`Connected to MongoDB database: ${dbName}`);
        return db;
    } catch (error: any) {
        logger.error("Failed to connect to MongoDB:", error);
        throw error;
    }
}

export function getMongoDB(): Db {
    if (!db) throw new Error("MongoDB not connected. Call connectMongo first.");
    return db;
}

export async function closeMongo(): Promise<void> {
    if (client) {
        await client.close();
        logger.info("MongoDB connection closed");
    }
}
