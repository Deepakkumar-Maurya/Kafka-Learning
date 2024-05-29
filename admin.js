const { kafka } = require("./client");

async function init() {
  try {
    const admin = kafka.admin();
    console.log("admin connecting...");
    admin.connect();
    console.log("admin connected ..");
  
    await admin.createTopics({
        topics: [
          {
            topic: "rider-updates",
            numPartitions: 2,
          },
        ],
      });
      console.log("admin createTopics done..");
      
      await admin.disconnect();
      console.log("admin disconnected..");
  } catch (error) {
    console.log(error.message);
  }
}

init();