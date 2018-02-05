// Prevent npm from looking up into the parent directory for node modules. If
// there is a missing dependency the default behavior will cause npm to look up
// the filesystem tree for it, which defeats the purpose of testing the package
// in a remote system
require('fs-lock')({
    'file_accessdir': [ __dirname ],
    'open_basedir': [ __dirname ]
});

import * as os from 'os';
import * as yargs from 'yargs';
import * as heapdump from 'heapdump';
import { Client, BrokeredMessage, ReceiveMode } from 'azure-servicebus';

const args = yargs
    .usage('npm start -- <connectionString> <queueName>', {
        count: {
            alias: 'c',
            number: true,
            describe: 'The number of messages to send in each iteration',
            default: 2e6 // 2 million
        },
        duration: {
            alias: 'd',
            number: true,
            describe: 'The period of time (in milliseconds) to allow each iteration to run',
            default: 1000 * 60 * 60 * 2 // 2 hours
        },
        iterations: {
            alias: 'i',
            number: true,
            describe: 'The number of consecutive iterations to execute before completing',
            default: 12
        },
        rate: {
            alias: 'r',
            number: true,
            describe: 'The rate at which messages should be sent in messages per second',
            default: 500
        },
        heapdump: {
            alias: 'h',
            number: true,
            describe: 'If set, the interval at which to take heap snapshots, in milliseconds'
        }
    })
    .demandCommand(2)
    .argv;

if (args.count / args.rate > args.duration / 1000) {
    console.error(`Not enough time to send all messages. The message send rate (${args.rate}/s) must be large enough that all messages (${args.count}) can be sent within the configured duration (${args.duration} ms)`);
    process.exit(1);
}

const client = Client.createFromConnectionString(args._[0]);
const queue = client.getQueue(args._[1]);

const messagesByIteration: { sent: number; failedSend: number; received: number; }[] = [];
const messagesByCompletedIteration: { sent: number; failedSend: number; received: number; }[] = [];

interface Body {
    messageIndex: number;
    iteration: number;
}

run().catch(e => {
    console.error('Encountered an unexpected error:');
    console.error(e);
    process.exit(1);
});

async function run() {
    let heapdumpTimer = null;
    if (args.heapdump) {
        heapdump.writeSnapshot((err, filename) => {
            console.log(`DUMP: ${filename}`);
        });

        heapdumpTimer = setInterval(() => {
            heapdump.writeSnapshot((err, filename) => {
                console.log(`DUMP: ${filename}`);
            });
        }, args.heapdump);
    }

    const listener = queue.onMessage((message: BrokeredMessage<Body>) => {
        messagesByIteration[message.body.iteration].received++;
        message.complete();
    }, {
        autoComplete: false,
        maxConcurrentCalls: 100,
        receiveMode: ReceiveMode.PeekLock
    });

    for (let i = 0; i < args.iterations; i++) {
        messagesByIteration[i] = { sent: 0, failedSend: 0, received: 0 };

        console.log('================================================');
        console.log(`STARTING ITERATION ${i}`);
        console.log('================================================');
        await runIteration(i);
        messagesByCompletedIteration[i] = JSON.parse(JSON.stringify(messagesByIteration[i]));
        console.log('================================================');
        console.log(`FINISHED ITERATION ${i}`);
        console.log(`Sent: ${messagesByCompletedIteration[i].sent}`);
        console.log(`Received: ${messagesByCompletedIteration[i].received}`);
        console.log(`Send Percentage: ${messagesByCompletedIteration[i].sent * 100 / args.count}%`);
        console.log(`Receive Percentage: ${messagesByCompletedIteration[i].received * 100 / messagesByCompletedIteration[i].sent}%`);
        console.log('================================================');
        console.log('\n');
    }

    // Tear down
    clearInterval(heapdumpTimer);
    listener.dispose();
    client.dispose();
}

async function runIteration(iteration: number) {
    const startTime = Date.now();

    const messagesPerBatch = args.rate / 10;
    let messageIndex = 0;
    let sentCount = 0;
    const sendInterval = setInterval(() => {
        const startIndex = messageIndex;
        for (; messageIndex < startIndex + messagesPerBatch && messageIndex < args.count; messageIndex++) {
            const message = new BrokeredMessage<Body>({ messageIndex, iteration });
            message.messageId = messageIndex.toString();
            queue.send(message)
                .then(() => messagesByIteration[iteration].sent++)
                .catch(() => messagesByIteration[iteration].failedSend++);
        }

        if (messageIndex >= args.count) {
            clearInterval(sendInterval);
        }
    }, 100);

    const statsInterval = setInterval(() => {
        console.log(`Time: ${Date.now() - startTime}\tSent: ${messagesByIteration[iteration].sent}\tReceived: ${messagesByIteration[iteration].received}\tCPU: ${os.loadavg()[0]}\tMem: ${process.memoryUsage().heapUsed}`)
    }, 60000);

    return new Promise((resolve, reject) => {
        setTimeout(() => {
            clearInterval(sendInterval);
            clearInterval(statsInterval);
            resolve();
        }, args.duration);
    });
}