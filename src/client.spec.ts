import {Client} from './';
import {ClientManager} from './clientManager';

describe('Client', () => {

    const endpointHost = 'sampleendpoint.servicebus.windows.net';
    const endpoint = `sb://${endpointHost}`;
    const keyName = 'sampleKeyName';
    const key = 'sampleKey';
    const connectionString = `Endpoint=${endpoint};SharedAccessKeyName=${keyName};SharedAccessKey=${key}`;

    let client: Client;

    beforeEach(() => {
        client = Client.createFromConnectionString(connectionString);
    });

    describe('#createFromConnectionString', () => {
        it('returns a client instance', () => {
            expect(client instanceof Client).toBe(true);
        });
    });

    describe('#getTopic', () => {
        it('initializes a topic', () => {
            const topic = client.getTopic('myTopic');
            expect(topic.topicName).toEqual('myTopic');
        });

        it('only creates one instance per topic', () => {
            const topic = client.getTopic('myTopic');
            const topic2 = client.getTopic('another');
            const topic3 = client.getTopic('myTopic');

            expect(topic).not.toBe(topic2);
            expect(topic).toBe(topic3);
        });
    });

    describe('#getQueue', () => {
        it('initializes a queue', () => {
            const queue = client.getQueue('myQueue');
            expect(queue.queueName).toEqual('myQueue');
        });

        it('only creates one instance per queue', () => {
            const queue = client.getQueue('myQueue');
            const queue2 = client.getQueue('another');
            const queue3 = client.getQueue('myQueue');

            expect(queue).not.toBe(queue2);
            expect(queue).toBe(queue3);
        });
    });

    describe('#dispose', () => {
        it('disconnects the client', done => {
            spyOn(ClientManager.prototype, 'dispose');
            client.dispose()
                .then(() => {
                    expect(ClientManager.prototype.dispose).toHaveBeenCalledTimes(1);
                })
                .catch(fail)
                .then(done);
        });
    });
});
