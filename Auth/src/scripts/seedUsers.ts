import { connectMongo } from '../infrastructure/mongo';
import { User } from '../models/user';

const seedUsers = async () => {
  await connectMongo();

  const users = Array.from({ length: 7000 }).map((_, i) => ({
    userId: `${i + 1}`,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`
  }));

  await User.insertMany(users);
  console.log('✅ Inserted 100 users');
  process.exit(0);
};

seedUsers();
