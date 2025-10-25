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
    onMessage?: (message: any, topic: string) => Promise<any>;
    topicDiscoveryInterval?: number;
    // NEW: Explicit topics to subscribe to
    subscribeToTopics?: string[];
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
        onMessage,
        topicDiscoveryInterval = 30000,
        subscribeToTopics = [],
    } = options;

    const kafka = new Kafka({ clientId: serviceName, brokers });
    let producer: Producer;
    let consumer: Consumer;
    let admin: Admin;
    let counter = 0;
    let consumerInitialized = false;
    let topicDiscoveryIntervalId: NodeJS.Timeout | null = null;

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
            try {
                admin = kafka.admin();
                await admin.connect();
                console.log(`[${serviceName}] Admin client connected for topic discovery`);
            } catch (error) {
                console.error(`[${serviceName}] Failed to connect admin client:`, error);
                return;
            }
        }

        try {
            const topics = await admin.listTopics();
            console.log(`[${serviceName}] 📋 Available topics in Kafka:`, topics);

            // More flexible pattern matching
            const responseTopicPatterns = [
                (t: string) => t.endsWith('.response'),
                (t: string) => t.endsWith('.res'),
                (t: string) => t.startsWith('rpc-responses'),
                (t: string) => t.includes('driver.location'), // Changed from regex
                (t: string) => t.includes(serviceName),
                (t: string) => t.endsWith('.request'),
                (t: string) => t.endsWith('.event'),
                (t: string) => t.endsWith('.events'), // Added for payment.events
                (t: string) => t.endsWith('.notification'),
            ];

            const relevantTopics = topics.filter(topic =>
                responseTopicPatterns.some(pattern => pattern(topic))
            );

            console.log(`[${serviceName}] 🎯 Relevant topics matched:`, relevantTopics);

            let newTopicsSubscribed = 0;

            for (const topic of relevantTopics) {
                if (!discoveredResponseTopics.has(topic)) {
                    try {
                        await consumer.subscribe({ topic, fromBeginning: false });
                        discoveredResponseTopics.add(topic);
                        newTopicsSubscribed++;
                        console.log(`[${serviceName}] ✅ Subscribed to: ${topic}`);
                    } catch (error: any) {
                        console.log(`[${serviceName}] ⚠️ Failed to subscribe: ${topic} - ${error.message}`);
                    }
                }
            }

            if (newTopicsSubscribed > 0) {
                console.log(`[${serviceName}] 🎉 Subscribed to ${newTopicsSubscribed} new topics`);
            }

            console.log(`[${serviceName}] 📊 Total subscribed topics: ${discoveredResponseTopics.size}`);
            console.log(`[${serviceName}] 📝 Subscribed topics list:`, Array.from(discoveredResponseTopics));

        } catch (error: any) {
            console.error(`[${serviceName}] ❌ Error discovering topics:`, error.message);
        }
    };

    const startTopicDiscovery = () => {
        if (topicDiscoveryIntervalId) {
            clearInterval(topicDiscoveryIntervalId);
        }

        // Trigger immediate discovery
        discoverAndSubscribeToTopics();

        topicDiscoveryIntervalId = setInterval(async () => {
            try {
                await discoverAndSubscribeToTopics();
            } catch (error: any) {
                console.error(`[${serviceName}] Topic discovery error:`, error.message);
            }
        }, topicDiscoveryInterval);

        console.log(`[${serviceName}] 🔄 Started topic discovery every ${topicDiscoveryInterval}ms`);
    };

    const stopTopicDiscovery = () => {
        if (topicDiscoveryIntervalId) {
            clearInterval(topicDiscoveryIntervalId);
            topicDiscoveryIntervalId = null;
            console.log(`[${serviceName}] ⏸️ Stopped topic discovery`);
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
        } catch (error: any) {
            console.log(`[${serviceName}] ⚠️ Failed to subscribe: ${responseTopic} - ${error.message}`);
        }
    };

    const initializeConsumer = async () => {
        if (consumerInitialized) return;

        consumer = kafka.consumer({
            groupId: `${serviceName}-consumer`,
            sessionTimeout: 30000,
            heartbeatInterval: 3000,
        });

        await consumer.connect();
        console.log(`[${serviceName}] 🔌 Consumer connected`);

        // Subscribe to explicit topics first
        if (subscribeToTopics.length > 0) {
            console.log(`[${serviceName}] 📌 Subscribing to explicit topics:`, subscribeToTopics);
            for (const topic of subscribeToTopics) {
                try {
                    await consumer.subscribe({ topic, fromBeginning: false });
                    discoveredResponseTopics.add(topic);
                    console.log(`[${serviceName}] ✅ Subscribed to explicit topic: ${topic}`);
                } catch (error: any) {
                    console.log(`[${serviceName}] ⚠️ Failed to subscribe to ${topic}: ${error.message}`);
                }
            }
        }

        // Perform initial topic discovery (non-blocking)
        console.log(`[${serviceName}] 🔍 Starting initial topic discovery...`);
        await discoverAndSubscribeToTopics();

        // Start periodic topic discovery
        startTopicDiscovery();

        await consumer.run({
            eachBatch: async ({ batch }) => {
                const topic = batch.topic;
                console.log(`\n[${serviceName}] 📨 Received ${batch.messages.length} messages from: ${topic}`);

                for (const msg of batch.messages) {
                    if (!msg?.value) continue;

                    try {
                        const payload = JSON.parse(msg.value.toString());
                        const correlationId = payload.correlationId || msg.headers?.correlationId?.toString();

                        console.log(`[${serviceName}] 📦 Message details:`, {
                            topic,
                            correlationId,
                            hasPayload: !!payload,
                            payloadKeys: Object.keys(payload)
                        });

                        // Check if this is an RPC response
                        const pending = pendingRequests.get(correlationId);

                        if (pending) {
                            // This is a response to our request
                            clearTimeout(pending.timeout);
                            const { __responseTopic, ...data } = payload;
                            pending.resolve(data);
                            pendingRequests.delete(correlationId);
                            console.log(`[${serviceName}] ✅ RPC Response matched: ${correlationId}`);
                        }
                        else if (onMessage) {
                            console.log(`[${serviceName}] 🔄 Processing event from: ${topic}`);
                            console.log(`[${serviceName}] 📄 Event data:`, JSON.stringify(payload, null, 2));

                            try {
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
                            } catch (handlerError: any) {
                                console.error(`[${serviceName}] ❌ Event handler error:`, handlerError.message);
                            }
                        } else {
                            console.log(`[${serviceName}] ⚠️ No handler for message: ${correlationId || 'no-id'} from topic: ${topic}`);
                        }

                    } catch (error: any) {
                        console.error(`[${serviceName}] ❌ Error processing message:`, error.message);
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
                console.log(`[${serviceName}] ⏱️ Timeout: ${correlationId}`);
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
        discoveredTopics: Array.from(discoveredResponseTopics),
        topicDiscoveryInterval: topicDiscoveryInterval,
        topicDiscoveryActive: !!topicDiscoveryIntervalId
    });

    const manualTopicDiscovery = async () => {
        console.log(`[${serviceName}] 🔍 Manual topic discovery triggered`);
        await discoverAndSubscribeToTopics();
    };

    return new Elysia({ name: 'kafka-rpc' })
        .decorate('kafkaRPC', {
            request,
            getPendingCount: () => pendingRequests.size,
            getQueueSize: () => queue.length,
            clearPendingRequests,
            getCacheStats,
            discoverTopics: discoverAndSubscribeToTopics,
            manualTopicDiscovery,
            startTopicDiscovery,
            stopTopicDiscovery,
            getSubscribedTopics: () => Array.from(discoveredResponseTopics)
        })
        .onStart(async () => {
            producer = kafka.producer();
            await producer.connect();
            console.log(`[${serviceName}] 🔌 Producer connected`);
            await initializeConsumer();
        })
        .onStop(async () => {
            stopTopicDiscovery();
            clearPendingRequests();
            await producer?.disconnect();
            await consumer?.disconnect();
            await admin?.disconnect();
            console.log(`[${serviceName}] 🔌 Kafka disconnected`);
        });
};