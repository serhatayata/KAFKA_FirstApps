const { Kafka } = require("kafkajs");

createProducer();

async function createProducer() {
  try {
    
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["192.168.56.1:9092"]
    });

    const producer = kafka.producer();
    console.log("Producer'a bağlanılıyor...");
    await producer.connect();
    console.log("Producer'a bağlantı başarılı");

    const message_result = await producer.send({
      topic: "raw_video_topic",
      messages: [
        {
          value:"Yeni video içeriği",
          partition:0
        }
      ]
    })

    console.log("Gonderim basarilidir", JSON.stringify(message_result));
    await producer.disconnect();


  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  } finally {
    process.exit(0);
  }
}