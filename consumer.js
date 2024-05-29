const { kafka } = require("./client");
const group = process.argv[2];

async function init() {
    try {
        const consumer = kafka.consumer({ groupId: group });
        console.log("consumer connecting...");
        await consumer.connect();
        console.log("consumer connected ..");
    
        await consumer.subscribe({ topics: ["rider-updates"], fromBeginning: true });
        console.log("consumer subscribed..");
    
        await consumer.run({
            eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                console.log({
                    group,
                    topic,
                    partition,
                    // offset: message.offset,
                    value: message.value.toString(),
                });
            },
        });
    } catch (error) {
        console.log(error.message);
    }
    // await consumer.disconnect();
}

init();