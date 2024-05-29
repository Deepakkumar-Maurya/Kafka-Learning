const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  try {
    const producer = kafka.producer();
  
    console.log("producer connecting...");
    await producer.connect();
    console.log("producer connected ..");
  
    rl.setPrompt('>');
    rl.prompt();
  
    rl.on("line", async function (line) {
      const [riderName, location] = line.split(" ");
      await producer.send({
        topic: "rider-updates",
        messages: [
          {
            partition: location.toLowerCase() === 'north' ? 0 : 1,
            key: "location-updates",
            value: JSON.stringify({ name: riderName, location }),
          },
        ],
      });
    }).on("close", async () => {
      console.log("closing..");
      await producer.disconnect();
    });
  } catch (error) {
    console.log(error.message);
  }
}
init();
