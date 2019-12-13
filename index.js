const app = require("express")();
const server = require("http").Server(app);

const path = require("path");
const fs = require("fs");
const kafka = require("kafka-node");
const Transform = require("stream").Transform;
const uuidv4 = require("uuid/v4");

const { kafka_server } = require("./kafkaConfig");

const PORT = process.env.PORT || 5000;

const topic = "images";

const consumerOptions = {
  kafkaHost: kafka_server,
  groupId: "ExampleTestGroup",
  sessionTimeout: 15000,
  protocol: ["roundrobin"],
  fetchMaxBytes: 1024 * 1024,
  fromOffset: "latest",
  encoding: "buffer",
  keyEncoding: "utf8",
  outOfRangeOffset: "earliest"
};

const streamArray = [];
let arrayLength = 0;
try {
  const kafkaConsumerStream = new kafka.ConsumerGroupStream(
    consumerOptions,
    topic
  );

  kafkaConsumerStream.on("data", chunk => {
    if (chunk.key.includes("last")) {
      arrayLength = Number(chunk.key.split(".")[0]);
    }
    streamArray.unshift(chunk);
    if (streamArray.length === arrayLength) {
      renderImage();
    }
  });

  const renderImage = () => {
    const { key } = streamArray.find(e => e.key.includes("last"));
    const file = key.split(".");
    const [keyName, fileName, fileExtension, fileSize] = file;

    console.log(`File Size is ${fileSize} bytes`);
    let filePath = path.join(
      process.cwd(),
      `repository/${fileName}.${fileExtension}`
    );
    const writeStream = fs.createWriteStream(filePath);
    let progress = 0;
    let percentage = 0;

    streamArray
      .sort((a, b) => a.key.split(".")[0] - b.key.split(".")[0])
      .forEach(chunk => {
        progress = progress + chunk.value.length;
        percentage = (progress / fileSize) * 100;
        console.log(
          `Uploading ${fileName} to respository ----- ${progress} bytes ---- ${Math.round(
            percentage
          )}%`
        );
        writeStream.write(chunk.value);
      });
    if (percentage === 100) {
      console.log("Upload is complete");
    }
    streamArray.length = 0;
  };
} catch (err) {
  console.log(err);
  throw err;
}

server.listen(PORT, () => console.log(`Server is listening to port ${PORT}`));
