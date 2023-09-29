const { kafka } = require('./client');
const group = process.argv[2];

async function init() {
    const consumer = kafka.consumer({ groupId: group });

    await consumer.connect();
    console.log(`Consumer '${group}' connected to Kafka`);

    await consumer.subscribe({ topic: 'hospital-tinent', fromBeginning: true });
    console.log(`Consumer '${group}' subscribed to 'hospital-tinent' topic`);

    console.log(`Consumer '${group}' created successfully`);
    

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            console.log(`part: ${partition}`),
            console.log(
                `${group}: [${topic}]: PART:${partition}:`,
                message.value.toString()
            );
        },
    });
}

init().catch((error) => {
    console.error(`Error in consumer '${group}':`, error);
});
