const app = require("express")();
const server = require("http").Server(app);

global.path = require("path");
global.fs = require("fs");
global.kafka = require("kafka-node");
global.Transform = require("stream").Transform;
global.uuidv4 = require("uuid/v4");

const { kafka_server } = require("./kafkaConfig");
const { kafkaToRepoConsumer, kafkaToClientConsumer } = require("./consumers");

global.PORT = process.env.PORT || 5000;

global.client = new kafka.KafkaClient({ kafkaHost: kafka_server });

// Process stream to repository
kafkaToRepoConsumer();

// Consume request files
kafkaToClientConsumer();


server.listen(PORT, () => console.log(`Server is listening to port ${PORT}`));
