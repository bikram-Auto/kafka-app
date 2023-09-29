const { kafka } = require('./client');

async function init() {
    const admin = kafka.admin();
    console.log('admin connecting...')
    
    try {
        await admin.connect();
        console.log('admin connection success');

        console.log('creating Topics [hospital-tinent]');
        await admin.createTopics({
            topics: [
                {
                    topic: "hospital-tinent",
                    numPartitions: 2,
                }
            ]
        });
        
        console.log('Topics [hospital-tinent] created successfully');
    } catch (error) {
        console.error('Error creating topics:', error);
    } finally {
        await admin.disconnect();
        console.log('admin disconnected');
    }
}

init().catch((error) => {
    console.error('Error in initialization:', error);
});
