import { Elysia } from 'elysia';
import { connectKafka, producer } from './infrastructure/kafka';
import { connectMongo } from './infrastructure/mongo';
import { startUserRequestConsumer } from './consumers/consumer';

const app = new Elysia();

app.get('/health', () => ({ status: 'Auth service running' }));

(async () => {
  try {
    console.log('🚀 Starting Auth service...');
    await connectMongo();
    await connectKafka();
    await producer.connect();
    await startUserRequestConsumer();
    app.listen(3001);
    console.log('Auth service listening on http://localhost:3001');
  } catch (err) {
    console.error(' Failed to start Auth service:', err);
    process.exit(1);
  }
})();
