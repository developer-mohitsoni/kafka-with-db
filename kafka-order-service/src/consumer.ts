import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import prisma from "./prisma.js";

dotenv.config();

const kafka = new Kafka({
  clientId: "order-consumer",
  brokers: [process.env.KAFKA_BROKER!],
});

const consumer = kafka.consumer({ groupId: "order-group" });

const consumeBulkOrders = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  console.log("🚀 Kafka Consumer Started...");

  await consumer.run({
    eachBatch: async ({ batch }) => {
      const orders = batch.messages.map((msg) =>
        JSON.parse(msg.value!.toString())
      );

      console.log(`✅ Processing ${orders.length} orders...`);

      if (orders.length > 0) {
        await prisma.order.createMany({
          data: orders,
          skipDuplicates: true,
        });
        console.log("📥 Orders inserted into PostgreSQL successfully!");
      }
    },
  });
};

consumeBulkOrders();
