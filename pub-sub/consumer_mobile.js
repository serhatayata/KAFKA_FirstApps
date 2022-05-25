const { Kafka } = require("kafkajs");


createConsumer();

async function createConsumer() {
  try {
    
    const kafka = new Kafka({
      clientId: "kafka_pub_sub_client",
      brokers: ["192.168.56.1:9092"]
    });

    const consumer = kafka.consumer(
      {
        groupId : "mobile_encoder_consumer_group"
      }
    );
    console.log("Consumer'a bağlanılıyor...");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı");

    //Consumer subscribe
    await consumer.subscribe({
      topic:"raw_video_topic",
      fromBeginning:true
    })

    await consumer.run({
      eachMessage: async result => {
        console.log(`İşlenen video : ${result.message.value}_mobile_encoder : Partition => ${result.partition} `)
      }
    })


  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  }
  
}