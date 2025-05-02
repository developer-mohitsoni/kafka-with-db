import { Kafka } from "kafkajs";

const kafka = new Kafka({
	clientId: "dlq-consumer",
	brokers: [process.env.KAFKA_BROKER as string],
});

const consumer = kafka.consumer({ groupId: "dlq-group" });

const consumeFailedOrders = async () => {
	await consumer.connect();
	await consumer.subscribe({ topic: "failed_orders", fromBeginning: true });

	await consumer.run({
		eachMessage: async ({ message }) => {
			if (!message.value) return;

			const order = JSON.parse(message.value.toString());
			console.log(`🚨 DLQ Order (Needs Manual Fix): ${order.id}`);
		},
	});
};

consumeFailedOrders().catch(console.error);
