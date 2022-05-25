const { Kafka } = require("kafkajs");


createConsumer();

async function createConsumer() {
  try {
    
    const kafka = new Kafka({
      clientId: "kafka_log_store_client",
      brokers: ["192.168.56.1:9092"]
    });

    const consumer = kafka.consumer(
      {
        groupId : "log_store_consumer_group"
      }
    );
    console.log("Consumer'a bağlanılıyor...");
    await consumer.connect();
    console.log("Consumer'a bağlantı başarılı");

    //Consumer subscribe
    await consumer.subscribe({
      topic:"LogStoreTopic",
      fromBeginning:true
    })

    await consumer.run({
      eachMessage: async result => {
        console.log(`Gelen mesaj : ${result.message.value} : Partition => ${result.partition} `)
      }
    })


  } catch (error) {
    console.log("Bir Hata Oluştu", error);
  }
  
}