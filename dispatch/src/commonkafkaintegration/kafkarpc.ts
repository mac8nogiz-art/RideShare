// commonkafkaintegration/kafkarpc.ts - Enhanced to handle both RPC and Events
import { Elysia } from 'elysia';
import { Kafka, Producer, Consumer, Admin } from 'kafkajs';

export interface KafkaRPCOptions {
    serviceName: string;
    brokers: string[];
    requestTopic?: string | ((data: any) => string);
    responseTopic?: string | ((data: any) => string);
    timeout?: number;
    batchSize?: number;
    batchTimeout?: number;
    //  NEW: Event handler for one-way messages
    onMessage?: (message: any, topic: string) => Promise<any>;
}

export const kafkaRPC = (options: KafkaRPCOptions) => {
    const {
        serviceName,
        brokers,
        requestTopic = 'rpc-requests',
        responseTopic = 'rpc-responses',
        timeout = 5000,
        batchSize = 1000,
        batchTimeout = 1,
        onMessage, // ✅ Event handler
    } = options;

    const kafka = new Kafka({ clientId: serviceName, brokers });
    let producer: Producer;
    let consumer: Consumer;
    let admin: Admin;
    let counter = 0;
    let consumerInitialized = false;

    const pendingRequests = new Map<string, {
        resolve: (data: any) => void;
        reject: (err: any) => void;
        timeout: NodeJS.Timeout;
    }>();

    let queue: Array<{
        data: any;
        correlationId: string;
        requestTopic: string;
        responseTopic: string;
    }> = [];
    let sending = false;

    const discoveredResponseTopics = new Set<string>();

    const genId = (prefix: string) => `${prefix}-${++counter}`;

    const resolveTopic = (topicDef: string | ((data: any) => string) | undefined, data: any, defaultTopic: string): string => {
        if (typeof topicDef === 'function') {
            return topicDef(data);
        }
        return topicDef || defaultTopic;
    };

    const flushQueue = async () => {
        if (sending || queue.length === 0) return;
        sending = true;

        const requestsByTopic = new Map<string, Array<{ data: any; correlationId: string }>>();

        queue.forEach(({ data, correlationId, requestTopic }) => {
            if (!requestsByTopic.has(requestTopic)) {
                requestsByTopic.set(requestTopic, []);
            }
            requestsByTopic.get(requestTopic)!.push({ data, correlationId });
        });

        queue = [];

        const sendPromises = Array.from(requestsByTopic.entries()).map(async ([topic, batch]) => {
            console.log(`[${serviceName}] Sending ${batch.length} requests to topic: ${topic}`);

            await producer.send({
                topic: topic,
                messages: batch.map(({ data, correlationId }) => ({
                    value: JSON.stringify({
                        ...data,
                        correlationId,
                        __responseTopic: resolveTopic(responseTopic, data, 'rpc-responses')
                    }),
                })),
            });
        });

        await Promise.all(sendPromises);
        sending = false;

        if (queue.length > 0) setImmediate(flushQueue);
    };

    const discoverAndSubscribeToTopics = async () => {
        if (!admin) {
            admin = kafka.admin();
            await admin.connect();
        }

        try {
            const topics = await admin.listTopics();
            console.log(`[${serviceName}] Available topics:`, topics);

            const responseTopicPatterns = [
                /.*\.response$/,
                /.*\.res$/,
                /^rpc-responses/,
                /driver\.location\..*/,  // ✅ Subscribe to driver locations
                new RegExp(`.*${serviceName}.*`)
            ];

            const relevantTopics = topics.filter(topic =>
                responseTopicPatterns.some(pattern => pattern.test(topic))
            );

            console.log(`[${serviceName}] Relevant topics:`, relevantTopics);

            for (const topic of relevantTopics) {
                if (!discoveredResponseTopics.has(topic)) {
                    try {
                        await consumer.subscribe({ topic, fromBeginning: false });
                        discoveredResponseTopics.add(topic);
                        console.log(`[${serviceName}] ✅ Subscribed to: ${topic}`);
                    } catch (error) {
                        console.log(`[${serviceName}] ❌ Failed to subscribe: ${topic}`);
                    }
                }
            }

        } catch (error) {
            console.error(`[${serviceName}] Error discovering topics:`, error);
        }
    };

    const subscribeToResponseTopic = async (responseTopic: string) => {
        if (discoveredResponseTopics.has(responseTopic)) {
            return;
        }

        try {
            await consumer.subscribe({ topic: responseTopic, fromBeginning: false });
            discoveredResponseTopics.add(responseTopic);
            console.log(`[${serviceName}] ✅ Subscribed to: ${responseTopic}`);
        } catch (error) {
            console.log(`[${serviceName}] ❌ Failed to subscribe: ${responseTopic}`);
        }
    };

    const initializeConsumer = async () => {
        if (consumerInitialized) return;

        consumer = kafka.consumer({ groupId: serviceName });
        await consumer.connect();

        // Initial subscription
        const initialPatterns = [
            /.*\.response$/,
            /.*\.request$/,  // ✅ Also listen to request topics
            /driver\.location\..*/,
            new RegExp(`${serviceName}.*`)
        ];

        for (const pattern of initialPatterns) {
            try {
                await consumer.subscribe({ topic: pattern, fromBeginning: false });
                console.log(`[${serviceName}] ✅ Subscribed to pattern: ${pattern}`);
            } catch (error) {
                console.log(`[${serviceName}] Pattern subscription skipped: ${pattern}`);
            }
        }

        await discoverAndSubscribeToTopics();

        await consumer.run({
            eachBatch: async ({ batch }) => {
                const topic = batch.topic;
                console.log(`[${serviceName}] 📥 Received ${batch.messages.length} messages from: ${topic}`);

                for (const msg of batch.messages) {
                    if (!msg?.value) continue;

                    try {
                        const payload = JSON.parse(msg.value.toString());
                        const correlationId = payload.correlationId || msg.headers?.correlationId?.toString();

                        // ✅ Check if this is an RPC response
                        const pending = pendingRequests.get(correlationId);

                        if (pending) {
                            // This is a response to our request
                            clearTimeout(pending.timeout);
                            const { __responseTopic, ...data } = payload;
                            pending.resolve(data);
                            pendingRequests.delete(correlationId);
                            console.log(`[${serviceName}] ✅ RPC Response matched: ${correlationId}`);
                        }
                        // ✅ If not an RPC response, treat as event
                        else if (onMessage) {
                            console.log(`[${serviceName}] 📨 Processing event from: ${topic}`);

                            // Call the event handler
                            const response = await onMessage(payload, topic);

                            // If there's a replyTo, send response back
                            const replyTo = msg.headers?.replyTo?.toString() || payload.__responseTopic;

                            if (replyTo && response) {
                                await producer.send({
                                    topic: replyTo,
                                    messages: [{
                                        key: payload.jobId || msg.key?.toString(),
                                        value: JSON.stringify(response),
                                        headers: {
                                            correlationId: correlationId || genId('resp'),
                                            timestamp: Date.now().toString()
                                        }
                                    }]
                                });
                                console.log(`[${serviceName}] ✅ Event response sent to: ${replyTo}`);
                            }
                        } else {
                            console.log(`[${serviceName}] ⚠️  No handler for message: ${correlationId || 'no-id'}`);
                        }

                    } catch (error) {
                        console.error(`[${serviceName}] ❌ Error processing message:`, error);
                    }
                }
            },
        });

        consumerInitialized = true;
        console.log(`[${serviceName}] ✅ Consumer ready with event handling`);
    };

    const request = <T = any>(data: any, requestId?: string): Promise<T> => {
        const correlationId = genId(requestId || 'req');

        const resolvedRequestTopic = resolveTopic(requestTopic, data, 'rpc-requests');
        const resolvedResponseTopic = resolveTopic(responseTopic, data, 'rpc-responses');

        if (consumerInitialized) {
            subscribeToResponseTopic(resolvedResponseTopic);
        }

        return new Promise((resolve, reject) => {
            const timeoutHandle = setTimeout(() => {
                console.log(`[${serviceName}] ⏱️  Timeout: ${correlationId}`);
                pendingRequests.delete(correlationId);
                reject(new Error(`Timeout: ${correlationId}`));
            }, timeout);

            pendingRequests.set(correlationId, { resolve, reject, timeout: timeoutHandle });

            queue.push({
                data: { ...data, __responseTopic: resolvedResponseTopic },
                correlationId,
                requestTopic: resolvedRequestTopic,
                responseTopic: resolvedResponseTopic
            });

            if (queue.length >= batchSize) {
                setImmediate(flushQueue);
            } else if (queue.length === 1) {
                setTimeout(flushQueue, batchTimeout);
            }
        });
    };

    const clearPendingRequests = () => {
        const count = pendingRequests.size;
        pendingRequests.forEach((pending) => {
            clearTimeout(pending.timeout);
            pending.reject(new Error(`Request cleared`));
        });
        pendingRequests.clear();
        return count;
    };

    const getCacheStats = () => ({
        pendingRequests: pendingRequests.size,
        queueSize: queue.length,
        consumerInitialized,
        isSending: sending,
        discoveredTopics: Array.from(discoveredResponseTopics)
    });

    return new Elysia({ name: 'kafka-rpc' })
        .decorate('kafkaRPC', {
            request,
            getPendingCount: () => pendingRequests.size,
            getQueueSize: () => queue.length,
            clearPendingRequests,
            getCacheStats,
            discoverTopics: discoverAndSubscribeToTopics,
        })
        .onStart(async () => {
            producer = kafka.producer();
            await producer.connect();
            await initializeConsumer();
        })
        .onStop(async () => {
            clearPendingRequests();
            await producer?.disconnect();
            await consumer?.disconnect();
            await admin?.disconnect();
        });
};