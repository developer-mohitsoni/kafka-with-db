import { Kafka } from "kafkajs";
import { faker } from "@faker-js/faker";
import dotenv from "dotenv";

dotenv.config();

// Kafka Setup
const kafka = new Kafka({
  clientId: "order-producer",
  brokers: [process.env.KAFKA_BROKER!],
});

const producer = kafka.producer();

const sendOrder = async () => {
  await producer.connect();

  for (let i = 0; i < 10; i++) {
    const order = {
      product: faker.commerce.product(),
      category: faker.commerce.department(), // 👈 Category ko Partition Key ke liye use karenge
      region: faker.location.country(),
      price: faker.commerce.price(),
    };

    await producer.send({
      topic: "orders",
      messages: [
        {
          key: order.category, // 👈 Partitioning Key
          value: JSON.stringify(order),
        },
      ],
    });

    console.log(`✅ Order Sent: ${JSON.stringify(order)}`);
  }

  await producer.disconnect();
};

sendOrder().catch(console.error);
