// import { consumer, producer } from '../infrastructure/kafka';
// import { User } from '../models/user';

// const cache = new Map<string, { data: any; exp: number }>();
// const MAX_CACHE = 10000;
// const CACHE_TTL = 300000;

// const setCache = (key: string, data: any) => {
//   if (cache.size >= MAX_CACHE) {
//     const first = cache.keys().next().value;
//     if (first) cache.delete(first);
//   }
//   cache.set(key, { data, exp: Date.now() + CACHE_TTL });
// };

// export const startUserRequestConsumer = async () => {
//   await consumer.subscribe({
//     topic: process.env.USER_REQUEST_TOPIC!,
//     fromBeginning: false,
//   });

//   console.log('[AUTH] Subscribed to topic:', process.env.USER_REQUEST_TOPIC);
//   console.log(' [AUTH] Consumer ready');

//   await consumer.run({
//     eachBatch: async ({ batch }) => {
//       console.log(`📥 [AUTH] Received batch of ${batch.messages.length} requests from Kafka`);
      
//       const requests = batch.messages
//         .filter(m => m?.value)
//         .map(m => JSON.parse(m.value!.toString()));

//       console.log(`[AUTH] Processing ${requests.length} user requests`);

//       const responses: any[] = [];
//       const dbLookups = new Map<string, string>();

//       // Check cache
//       for (const { userId, correlationId } of requests) {
//         const cached = cache.get(userId);
//         if (cached && cached.exp > Date.now()) {
//           console.log(`[AUTH] Cache HIT for user ${userId}`);
//           responses.push({
//             value: JSON.stringify({ correlationId, user: cached.data }),
//           });
//         } else {
//           console.log(`[AUTH] Cache MISS for user ${userId} - will query DB`);
//           dbLookups.set(userId, correlationId);
//         }
//       }

//       // Batch DB query
//       if (dbLookups.size > 0) {
//         console.log(`🗄️  [AUTH] Querying DB for ${dbLookups.size} users...`);
//         const users = await User.find(
//           { userId: { $in: Array.from(dbLookups.keys()) } }
//         ).lean();

//         console.log(`[AUTH] Found ${users.length} users in DB`);

//         users.forEach((user: any) => {
//           const correlationId = dbLookups.get(user.userId);
//           if (!correlationId) return;
          
//           setCache(user.userId, user);
//           console.log(`[AUTH] Cached user ${user.userId} - ${user.name}`);
//           responses.push({
//             value: JSON.stringify({
//               correlationId,
//               user,
//             }),
//           });
//         });
//       }

//       // Send responses via Kafka
//       if (responses.length > 0) {
//         console.log(`📤 [AUTH] Sending ${responses.length} responses to Kafka topic: ${process.env.USER_RESPONSE_TOPIC}`);
//         await producer.send({
//           topic: process.env.USER_RESPONSE_TOPIC ?? 'user.response',
//           messages: responses,
//         });
//         console.log(`[AUTH] Sent ${responses.length} responses via Kafka`);
//       }
//     },
//   });
// };

import { consumer, producer } from '../infrastructure/kafka';
import { User } from '../models/user';

const cache = new Map<string, { data: any; exp: number }>();
const MAX_CACHE = 10000;
const CACHE_TTL = 300000;

const setCache = (key: string, data: any) => {
  if (cache.size >= MAX_CACHE) {
    const first = cache.keys().next().value;
    if (first) cache.delete(first);
  }
  cache.set(key, { data, exp: Date.now() + CACHE_TTL });
};

export const startUserRequestConsumer = async () => {
  // Subscribe to all possible request topics using patterns
  // This assumes your topics follow some naming convention
  await consumer.subscribe({
    topic: /.*\.request$|^request\.|^rpc\.request/, // Common patterns for request topics
    fromBeginning: false,
  });

  // Alternative: Subscribe to a wildcard pattern for all topics
  // await consumer.subscribe({ topic: /.*/, fromBeginning: false });

  console.log('[AUTH] Subscribed to request topic patterns');
  console.log('[AUTH] Consumer ready');

  await consumer.run({
    eachBatch: async ({ batch }) => {
      const requestTopic = batch.topic;
      console.log(`📥 [AUTH] Received batch of ${batch.messages.length} requests from topic: ${requestTopic}`);
      
      const requests = batch.messages
        .filter(m => m?.value)
        .map(m => {
          try {
            const parsed = JSON.parse(m.value!.toString());
            return {
              ...parsed,
              __requestTopic: requestTopic // Include the topic this message came from
            };
          } catch (error) {
            console.error('[AUTH] Error parsing message:', error);
            return null;
          }
        })
        .filter(Boolean);

      console.log(`[AUTH] Processing ${requests.length} user requests`);

      const responsesByTopic = new Map<string, any[]>();
      const dbLookups = new Map<string, { 
        correlationId: string; 
        responseTopic: string;
        originalData: any;
      }>();

      // Process each request and determine response topic
      for (const request of requests) {
        const { userId, correlationId, __responseTopic, ...data } = request;
        
        if (!userId || !correlationId) {
          console.log('[AUTH] Skipping invalid request - missing userId or correlationId');
          continue;
        }

        // Determine response topic dynamically
        let responseTopic: string;
        
        if (__responseTopic) {
          // Use the response topic specified in the message
          responseTopic = __responseTopic;
        } else {
          // Derive response topic from request topic
          // Example: "user.request" -> "user.response"
          // Example: "dispatch.user.request" -> "dispatch.user.response"
          responseTopic = requestTopic
            .replace(/\.request$/, '.response')
            .replace(/\.req$/, '.res')
            .replace(/^request\./, 'response.');
          
          // Fallback if no pattern matches
          if (responseTopic === requestTopic) {
            responseTopic = 'user.response'; // Ultimate fallback
          }
        }

        console.log(`[AUTH] Request ${correlationId} from ${requestTopic} -> response to ${responseTopic}`);

        const cached = cache.get(userId);
        if (cached && cached.exp > Date.now()) {
          console.log(`[AUTH] Cache HIT for user ${userId}`);
          
          if (!responsesByTopic.has(responseTopic)) {
            responsesByTopic.set(responseTopic, []);
          }
          
          responsesByTopic.get(responseTopic)!.push({
            value: JSON.stringify({ 
              correlationId, 
              user: cached.data,
              __requestTopic: requestTopic
            }),
          });
        } else {
          console.log(`[AUTH] Cache MISS for user ${userId} - will query DB`);
          dbLookups.set(userId, { 
            correlationId, 
            responseTopic,
            originalData: data
          });
        }
      }

      // Batch DB query for cache misses
      if (dbLookups.size > 0) {
        console.log(`🗄️  [AUTH] Querying DB for ${dbLookups.size} users...`);
        const users = await User.find(
          { userId: { $in: Array.from(dbLookups.keys()) } }
        ).lean();

        console.log(`[AUTH] Found ${users.length} users in DB`);

        users.forEach((user: any) => {
          const lookup = dbLookups.get(user.userId);
          if (!lookup) return;
          
          const { correlationId, responseTopic, originalData } = lookup;
          
          setCache(user.userId, user);
          console.log(`[AUTH] Cached user ${user.userId} - ${user.name}`);
          
          if (!responsesByTopic.has(responseTopic)) {
            responsesByTopic.set(responseTopic, []);
          }
          
          responsesByTopic.get(responseTopic)!.push({
            value: JSON.stringify({
              correlationId,
              user,
              __requestTopic: requestTopic,
              ...originalData // Include original data for context
            }),
          });
        });

        // Handle users not found in DB
        dbLookups.forEach((lookup, userId) => {
          if (!users.find((u: any) => u.userId === userId)) {
            console.log(`[AUTH] User ${userId} not found in DB`);
            const { correlationId, responseTopic, originalData } = lookup;
            
            if (!responsesByTopic.has(responseTopic)) {
              responsesByTopic.set(responseTopic, []);
            }
            
            responsesByTopic.get(responseTopic)!.push({
              value: JSON.stringify({
                correlationId,
                error: 'User not found',
                __requestTopic: requestTopic,
                ...originalData
              }),
            });
          }
        });
      }

      // Send responses to their respective topics
      let totalSent = 0;
      const sendPromises = Array.from(responsesByTopic.entries()).map(async ([responseTopic, messages]) => {
        console.log(`📤 [AUTH] Sending ${messages.length} responses to topic: ${responseTopic}`);
        
        try {
          await producer.send({
            topic: responseTopic,
            messages: messages,
          });
          
          console.log(` [AUTH] Sent ${messages.length} responses to ${responseTopic}`);
          totalSent += messages.length;
        } catch (error) {
          console.error(`[AUTH] Failed to send responses to ${responseTopic}:`, error);
        }
      });

      await Promise.all(sendPromises);
      console.log(`[AUTH] Successfully sent ${totalSent} total responses across ${responsesByTopic.size} topics`);
    },
  });
};