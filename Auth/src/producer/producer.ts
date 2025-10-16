// import { producer } from '../infrastructure/kafka';
// import { randomUUID } from 'crypto';

// export async function requestUserDetails(userId: string) {
//   const correlationId = randomUUID();

//   await producer.send({
//     topic: 'auth.user.request',
//     messages: [
//       {
//         key: correlationId,
//         value: JSON.stringify({ userId, correlationId }),
//       },
//     ],
//   });

//   return correlationId;
// }
