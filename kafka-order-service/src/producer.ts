import { Kafka } from "kafkajs";
import { faker } from "@faker-js/faker";
import dotenv from "dotenv";

dotenv.config();

const kafka = new Kafka({
  clientId: "order-producer",
  brokers: [process.env.KAFKA_BROKER!],
});

const producer = kafka.producer();

const generateOrder = () => ({
  userId: faker.string.uuid(),
  productId: faker.commerce.product(),
  quantity: faker.number.int({ min: 1, max: 10 }),
});

const sendBulkOrders = async (count: number) => {
  await producer.connect();

  const messages = Array.from({ length: count }).map(() => ({
    key: faker.string.uuid(),
    value: JSON.stringify(generateOrder()),
  }));

  await producer.send({
    topic: "orders",
    messages,
  });

  console.log(`✅ Sent ${count} orders to Kafka`);
  await producer.disconnect();
};

// Send 1000 Orders
sendBulkOrders(1000);
