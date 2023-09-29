const { kafka } = require('./client');
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

async function init() {
    const producer = kafka.producer();

    console.log('Connecting Producer');
    await producer.connect();
    console.log('Producer connected successfully');

    rl.setPrompt('> ');
    rl.prompt();

    rl.on('line', async function (line) {
        const [patientName, tinient] = line.split(" ");

        if (patientName && tinient) {
            let partition;
            if (tinient.toLocaleLowerCase() === 'apollo') {
                partition = 0;
            } else if (tinient.toLocaleLowerCase() === 'care') {
                partition = 1;
            } else {
                
                console.error('Invalid tinient. Supported values are "apollo" and "care".');
                return;
            }

            await producer.send({
                topic: 'hospital-tinent',
                messages: [
                    {
                        partition: partition,
                        key: 'tinient-update',
                        value: JSON.stringify({ name: patientName, tinient: tinient }),
                    },
                ],
            });
        } else {
            console.error('Invalid input format. Please enter patientName and tinient.');
        }
    });
}

init().catch((error) => {
    console.error('Error in initialization:', error);
});
