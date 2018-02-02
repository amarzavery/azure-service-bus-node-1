import { ClientManager, CreateLinkGroupOptions } from './clientManager';
import * as constants from './constants';
import { debug } from './utility';

import { mockSender, mockReceiver } from './test/mocks';
import { tickPromise } from './test/util';

describe('ClientManager', () => {
    const fakeAmqpClient = class FakeAmqpClient {

        public constructorCallCount = 0;
        public static instance = {};

        constructor() {
            this.constructorCallCount++;
        }

        connect(url: string): Promise<FakeAmqpClient> {
            return Promise.resolve(this);
        }

        disconnect(): Promise<void> {
            return Promise.resolve();
        }

        createSender(): Promise<any> {
            return Promise.resolve(mockSender);
        }

        createReceiver(): Promise<any> {
            return Promise.resolve(mockReceiver);
        }
    };

    let realAmqpClient: any;

    const endpointHost = 'sampleendpoint.servicebus.windows.net';
    const endpoint = `sb://${endpointHost}`;
    const keyName = 'sampleKeyName';
    const key = 'sampleKey';
    const connectionString = `Endpoint=${endpoint};SharedAccessKeyName=${keyName};SharedAccessKey=${key}`;

    let clientManager: ClientManager;

    let createClientCalls: number;

    beforeEach(() => {
        // Mock out the amqp client
        realAmqpClient = require('@azure-iot/amqp10').Client;
        require('@azure-iot/amqp10').Client = fakeAmqpClient;

        spyOn(fakeAmqpClient.prototype, 'connect').and.callThrough();
        spyOn(fakeAmqpClient.prototype, 'disconnect').and.callThrough();
        spyOn(fakeAmqpClient.prototype, 'createSender').and.callThrough();
        spyOn(fakeAmqpClient.prototype, 'createReceiver').and.callThrough();
        clientManager = new ClientManager(connectionString, debug);
        jasmine.clock().install();
    });

    afterEach(() => {
        require('@azure-iot/amqp10').Client = realAmqpClient;
        jasmine.clock().uninstall();
    });

    describe('#getAMQPClient', () => {
        it('returns a new AMQP client', (done: DoneFn) => {
            clientManager.getAMQPClient().client
                .then(client => {
                    expect(fakeAmqpClient.prototype.connect).toHaveBeenCalledTimes(1);
                }).catch(fail)
                .then(done);
        });

        it('returns same AMQP client if links available', (done: DoneFn) => {
            clientManager.getAMQPClient().client
                .then(client1 => {
                    return clientManager.getAMQPClient().client
                        .then(client2 => {
                            expect(client1).toBe(client2);
                            done();
                        });
                }).catch(fail);
        });

        it('returns new AMQP client if previous client full for concurrent requests', (done: DoneFn) => {
            spyOn(constants.default, 'handleMax').and.returnValue(1);
            Promise.all([clientManager.getAMQPClient().client, clientManager.getAMQPClient().client])
                .then(clients => {
                    expect(clients[0]).not.toBe(clients[1]);
                }).catch(fail)
                .then(done);
        });

        it('returns new AMQP client if previous client full', (done: DoneFn) => {
            spyOn(constants.default, 'handleMax').and.returnValue(1);
            clientManager.getAMQPClient().client
                .then(client1 => {
                    return clientManager.getAMQPClient().client
                        .then(client2 => {
                            expect(client1).not.toBe(client2);
                            done();
                        });
                }).catch(fail);
        });

        it('runs cleanup on dispose and no links', (done: DoneFn) => {
            const clientPromise = clientManager.getAMQPClient();
            clientPromise.client
                .then(client => {
                    clientPromise.dispose();
                    jasmine.clock().tick(constants.default.amqpClientCleanupDelayMs);
                    return tickPromise(5);
                }).then(() => {
                    expect(fakeAmqpClient.prototype.disconnect).toHaveBeenCalledTimes(1);
                }).catch(fail)
                .then(done);
        });

        it('runs cleanup on dispose and no links only once - case1', (done: DoneFn) => {
            const clientPromise1 = clientManager.getAMQPClient();
            clientPromise1.client
                .then(client1 => {
                    const clientPromise2 = clientManager.getAMQPClient();
                    return clientPromise2.client
                        .then(client2 => {
                            clientPromise1.dispose();
                            clientPromise2.dispose();
                            jasmine.clock().tick(constants.default.amqpClientCleanupDelayMs);
                            return tickPromise(5);
                        }).then(() => {
                            expect(fakeAmqpClient.prototype.disconnect).toHaveBeenCalledTimes(1);
                            done();
                        });
                }).catch(fail);
        });

        it('runs cleanup on dispose and no links only once - case2', (done: DoneFn) => {
            const clientPromise1 = clientManager.getAMQPClient();
            let clientPromise2: any;
            clientPromise1.client
                .then(client1 => {
                    clientPromise1.dispose();
                    clientPromise2 = clientManager.getAMQPClient();
                    return clientPromise2.client;
                }).then(client2 => {
                    clientPromise2.dispose();
                    jasmine.clock().tick(constants.default.amqpClientCleanupDelayMs);
                    return tickPromise(5);
                }).then(() => {
                    expect(fakeAmqpClient.prototype.disconnect).toHaveBeenCalledTimes(1);
                }).catch(fail)
                .then(done);
        });

        it('does not run cleanup on dispose if links remaining - case1', (done: DoneFn) => {
            const clientPromise = clientManager.getAMQPClient();
            clientPromise.client
                .then(client => {
                    clientPromise.dispose();
                    return clientManager.getAMQPClient().client;
                }).then(() => {
                    jasmine.clock().tick(constants.default.amqpClientCleanupDelayMs);
                    return tickPromise(5);
                }).then(() => {
                    expect(fakeAmqpClient.prototype.disconnect).not.toHaveBeenCalled();
                }).catch(fail)
                .then(done);
        });

        it('does not run cleanup on dispose if links remaining - case2', (done: DoneFn) => {
            const clientPromise = clientManager.getAMQPClient();
            clientPromise.client
                .then(client => {
                    return clientManager.getAMQPClient().client
                        .then(() => {
                            clientPromise.dispose();
                            jasmine.clock().tick(constants.default.amqpClientCleanupDelayMs);
                            return tickPromise(5);
                        }).then(() => {
                            expect(fakeAmqpClient.prototype.disconnect).not.toHaveBeenCalled();
                            done();
                        });
                }).catch(fail);
        });
    });

    describe('#dispose', () => {
        it('disposes all AMQP clients', (done: DoneFn) => {
            clientManager.getAMQPClient().client
                .then(() => {
                    return clientManager.dispose()
                        .then(() => {
                            expect(fakeAmqpClient.prototype.disconnect).toHaveBeenCalledTimes(1);
                            done();
                        });
                }).catch(fail);
        });
    });
});