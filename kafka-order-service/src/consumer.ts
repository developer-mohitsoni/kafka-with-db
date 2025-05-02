import dotenv from "dotenv";
import { Kafka } from "kafkajs";
import type { Order } from "./index.d.js";
import prisma from "./prisma.js";

dotenv.config();

const kafka = new Kafka({
	clientId: "order-consumer",
	brokers: [process.env.KAFKA_BROKER as string],
});

const consumer = kafka.consumer({ groupId: "order-group" });

const MAX_RETRIES = 3; // 👈 Maximum 3 retries

const processOrder = async (order: Order, retryCount = 0) => {
	try {
		await prisma.order.create({
			data: {
				product: order.product,
				category: order.category,
				region: order.region,
				price: order.price,
			},
		});

		console.log(`✅ Order Inserted: ${order.product}`);
	} catch (error) {
		console.error(
			`❌ Order Insert Failed: ${order.product}, Retry: ${retryCount}`,
		);

		if (retryCount < MAX_RETRIES) {
			// Retry after 2 sec
			setTimeout(() => processOrder(order, retryCount + 1), 2000);
		} else {
			console.error(`🚨 Moving Order to DLQ: ${order.id}`);
			await kafka.producer().send({
				topic: "failed_orders", // 👈 DLQ topic me bhejna
				messages: [{ key: order.category, value: JSON.stringify(order) }],
			});
		}
	}
};

const consumeOrders = async () => {
	await consumer.connect();
	await consumer.subscribe({ topic: "orders", fromBeginning: true });

	await consumer.run({
		eachMessage: async ({ message }) => {
			if (!message.value) return;

			const order = JSON.parse(message.value.toString());
			await processOrder(order);
		},
	});
};

consumeOrders().catch(console.error);
