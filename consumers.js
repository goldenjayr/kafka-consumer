const { kafka_server } = require("./kafkaConfig");

const kafkaToRepoConsumer = () => {
  const consumerOptions = {
    kafkaHost: kafka_server,
    groupId: "ExampleTestGroup",
    sessionTimeout: 15000,
    protocol: ["roundrobin"],
    fetchMaxBytes: 1024 * 1024 * 10,
    fromOffset: "latest",
    encoding: "buffer",
    keyEncoding: "utf8",
    outOfRangeOffset: "earliest"
  };

  try {
    const kafkaConsumerStream = new kafka.ConsumerGroupStream(
      consumerOptions,
      "images"
    );
    const fileList = {};
    const fileNumberOfChunks = {};

    kafkaConsumerStream.on("data", chunk => {
      const key = JSON.parse(chunk.key);
      if (key.last) {
        fileNumberOfChunks[`${key.fileName}`] = Number(key.order);
        console.log("TCL: fileNumberOfChunks", fileNumberOfChunks);
      }
      if (!fileList[`${key.fileName}`]) fileList[`${key.fileName}`] = [];
      chunk.key = key;
      fileList[`${key.fileName}`].unshift(chunk);

      if (
        fileList[`${key.fileName}`].length ===
        fileNumberOfChunks[`${key.fileName}`]
      ) {
        renderImage(key.fileName);
      }
    });

    const renderImage = filename => {
      const { key } = fileList[`${filename}`].find(e => e.key);
      const { fileName, fileSize } = key;
      console.log(`File Size is ${fileSize} bytes`);
      let filePath = path.join(process.cwd(), `../file-repository/${fileName}`);
      const writeStream = fs.createWriteStream(filePath);
      let progress = 0;
      let percentage = 0;

      fileList[`${filename}`]
        .sort((a, b) => a.key.order - b.key.order)
        .forEach(chunk => {
          console.log("ORDERED ARRAY --> ", {
            partition: chunk.partition,
            key: chunk.key.order
          });
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
      fileList[`${filename}`].length = 0;
    };
  } catch (err) {
    console.log(err);
    throw err;
  }
};

const kafkaToClientConsumer = () => {
  try {
    const consumerOptions = {
      groupId: "kafkaGroup",
      sessionTimeout: 15000,
      protocol: ["roundrobin"],
      fetchMaxBytes: 1024 * 1024 * 10,
      fromOffset: "latest",
      outOfRangeOffset: "earliest"
    };

    const requestFileConsumerGroup = new kafka.ConsumerGroup(
      consumerOptions,
      "request-files"
    );
    requestFileConsumerGroup.on("message", data => {
      const fileDir = path.join(process.cwd(), "../file-repository");

      fs.readdir(fileDir, (err, files) => {
        if (err) {
          return console.log("Unable to scan directory: " + err);
        }

        files.forEach(file => {
          let order = 0;
          const streamArray = [];
          const producerStream = new kafka.ProducerStream(client);
          const filePath = path.resolve(fileDir, file);
          const readStream = fs.createReadStream(filePath);
          const { size } = fs.statSync(filePath);
          const streamToTopic = new Transform({
            objectMode: true,
            decodeStrings: true,
            transform(chunk, encoding, callback) {
              order = order + 1;
              callback(null, {
                key: JSON.stringify({
                  order: order,
                  fileName: file,
                  fileSize: size
                }),
                topic: "image-request",
                messages: chunk
              });
            }
          });

          readStream.pipe(streamToTopic).on("data", chunk => {
            streamArray.unshift(chunk);
          });

          readStream.on("end", () => {
            const key = JSON.parse(streamArray[0].key);
            key.last = true;
            streamArray[0].key = JSON.stringify(key);
            // created new transform because producerStream cannot push from array
            const newTransformStream = new Transform({
              objectMode: true,
              decodeStrings: true,
              transform(chunk, encoding, callback) {
                callback(null, {
                  messages: chunk
                });
              }
            });
            streamArray.forEach(arr => {
              newTransformStream.push(arr);
            });
            newTransformStream.pipe(producerStream);
          });
        });
      });
    });
  } catch (error) {
    if (error) throw error;
  }
};

module.exports = {
  kafkaToRepoConsumer,
  kafkaToClientConsumer
};
