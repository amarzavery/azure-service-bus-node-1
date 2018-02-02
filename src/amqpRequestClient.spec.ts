import * as uuid from 'node-uuid';

import { AmqpRequestClient } from './amqpRequestClient';
import constants from './constants';
import { mockAmqpClientManager, mockAmqpClient, mockSender, mockReceiver, mockSession } from './test/mocks';
import { ErrorNames } from './errors';
import { debug } from './utility';

describe('AmqpRequestClient', () => {

    let client: AmqpRequestClient;
    const topicName = 'topic';
    const subscriptionName = 'subscription';

    beforeEach(done => {
        spyOn(mockAmqpClientManager, 'getAMQPClient').and.callThrough();
        spyOn(mockAmqpClient, 'createSession').and.callThrough();
        spyOn(mockAmqpClient, 'createSender').and.callThrough();
        spyOn(mockAmqpClient, 'createReceiver').and.callThrough();

        AmqpRequestClient.create(`${topicName}/Subscriptions/${subscriptionName}`, mockAmqpClientManager, debug)
            .then(amqpRequestClient => {
                client = amqpRequestClient;
                setTimeout(() => {
                    done();
                }, 1);
            });
    });

    afterEach(() => {
        client.dispose();
    });

    it('creates a sender and receiver link pair', () => {
        expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
        expect(mockAmqpClient.createSession).toHaveBeenCalledTimes(1);
        expect(mockAmqpClient.createSender).toHaveBeenCalledTimes(1);
        expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
    });

    it('does not throw when the links detach', () => {
        mockSender.emit('detached', { err: 'boom!' });
        mockReceiver.emit('detached', { err: 'boom!' });
    });

    it('emits an error if a response is received for a request that was never sent', done => {
        client.on(AmqpRequestClient.REQUEST_CLIENT_ERROR, (err) => {
            expect(err.name).toEqual(ErrorNames.Internal.OrphanedResponse);
            done();
        });

        mockReceiver.sendMessage({
            properties: {
                correlationId: uuid.v4()
            },
            applicationProperties: {
                statusCode: 200,
                statusDescription: 'Okay',
                errorCondition: 'foo',
                'com.microsoft:tracking-id': uuid.v4()
            },
            body: {
                'expirations': [new Date()]
            }
        });
    });

    describe('#renewLock', () => {

        it('sends a message using the request sender', async () => {
            spyOn(mockSender, 'send').and.callFake(() => {
                const message = mockSender.send.calls.argsFor(0)[0];
                expect(message.properties.messageId).toBeDefined();
                expect(message.properties.replyTo).toEqual(mockReceiver.name);
                expect(message.body).toBeDefined();

                mockReceiver.sendMessage({
                    properties: {
                        correlationId: message.properties.messageId
                    },
                    applicationProperties: {
                        statusCode: 200,
                        statusDescription: 'Okay',
                        errorCondition: 'foo',
                        'com.microsoft:tracking-id': uuid.v4()
                    },
                    body: {
                        'expirations': [new Date()]
                    }
                });
                return Promise.resolve();
            });

            await client.renewLock(uuid.v4());
            expect(mockSender.send).toHaveBeenCalledTimes(1);
        });

        it('ignores extra attaches for the sender and receiver links', async () => {
            mockSender.emit('attached');
            mockReceiver.emit('attached');

            spyOn(mockSender, 'send').and.callFake(() => {
                const message = mockSender.send.calls.argsFor(0)[0];
                expect(message.properties.messageId).toBeDefined();
                expect(message.properties.replyTo).toEqual(mockReceiver.name);
                expect(message.body).toBeDefined();

                mockReceiver.sendMessage({
                    properties: {
                        correlationId: message.properties.messageId
                    },
                    applicationProperties: {
                        statusCode: 200,
                        statusDescription: 'Okay',
                        errorCondition: 'foo',
                        'com.microsoft:tracking-id': uuid.v4()
                    },
                    body: {
                        'expirations': [new Date()]
                    }
                });
                return Promise.resolve();
            });

            await client.renewLock(uuid.v4());
            expect(mockSender.send).toHaveBeenCalledTimes(1);
        });

        it('still works after the underlying links have been detached and reattached', async () => {
            mockSender.emit('detached', 'foo');
            mockReceiver.emit('detached', 'foo');
            mockSender.emit('attached');
            mockReceiver.emit('attached');

            spyOn(mockSender, 'send').and.callFake(() => {
                const message = mockSender.send.calls.argsFor(0)[0];
                expect(message.properties.messageId).toBeDefined();
                expect(message.properties.replyTo).toEqual(mockReceiver.name);
                expect(message.body).toBeDefined();

                mockReceiver.sendMessage({
                    properties: {
                        correlationId: message.properties.messageId
                    },
                    applicationProperties: {
                        statusCode: 200,
                        statusDescription: 'Okay',
                        errorCondition: 'foo',
                        'com.microsoft:tracking-id': uuid.v4()
                    },
                    body: {
                        'expirations': [new Date()]
                    }
                });
                return Promise.resolve();
            });

            await client.renewLock(uuid.v4());
            expect(mockSender.send).toHaveBeenCalledTimes(1);
        });

        it('rejects when the sender can\'t send', done => {
            spyOn(mockSender, 'canSend').and.returnValue(false);
            spyOn(mockSender, 'send').and.callThrough();
            client.renewLock(uuid.v4())
                .then(() => new Error('Did not fail when expected to'))
                .catch(err => {
                    expect(mockSender.send).not.toHaveBeenCalled();
                    expect(err.name).toEqual(ErrorNames.Internal.RequestFailure);
                })
                .catch(fail)
                .then(done);
        });

        it('rejects the promise when the response indicates an error', done => {
            spyOn(mockSender, 'send').and.callFake(() => {
                const message = mockSender.send.calls.argsFor(0)[0];
                expect(message.properties.messageId).toBeDefined();
                expect(message.properties.replyTo).toEqual(mockReceiver.name);
                expect(message.body).toBeDefined();

                mockReceiver.sendMessage({
                    properties: {
                        correlationId: message.properties.messageId
                    },
                    applicationProperties: {
                        statusCode: 500,
                        statusDescription: 'Internal error',
                        errorCondition: 'foo',
                        'com.microsoft:tracking-id': uuid.v4()
                    },
                    body: {
                        'expirations': [new Date()]
                    }
                });
                return Promise.resolve();
            });

            client.renewLock(uuid.v4())
                .then(() => new Error('Did not fail when expected to'))
                .catch(err => {
                    expect(mockSender.send).toHaveBeenCalledTimes(1);
                    expect(err.name).toEqual(ErrorNames.Internal.RequestFailure);
                })
                .catch(fail)
                .then(done);
        });

        it('rejects if the call times out', done => {
            spyOn(mockSender, 'send').and.callThrough();

            jasmine.clock().install();

            client.renewLock(uuid.v4())
                .then(() => new Error('Did not fail when expected to'))
                .catch(err => {
                    expect(mockSender.send).toHaveBeenCalledTimes(1);
                    expect(err.name).toEqual(ErrorNames.Internal.RequestTimeout);
                    expect(err.context.status).toEqual(504); // gateway timeout
                })
                .catch(err => {
                    fail(err);
                })
                .then(done);

            jasmine.clock().tick(constants.amqpRequestTimeout + 1);
            jasmine.clock().uninstall();
        });

        it('rejects if sending the request fails', done => {
            spyOn(mockSender, 'send').and.returnValue(Promise.reject(new Error('boom!')));

            client.renewLock(uuid.v4())
                .then(() => new Error('Did not fail when expected to'))
                .catch(err => {
                    expect(mockSender.send).toHaveBeenCalledTimes(1);
                    expect(err.name).toEqual(ErrorNames.Internal.RequestFailure);
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('#destroy', () => {
        it('ends the session', () => {
            spyOn(mockSession, 'end');

            // Kick a request off so that there is some state hanging around
            client.renewLock(uuid.v4()).catch(() => { });

            client.dispose();
            expect(mockSession.end).toHaveBeenCalled();
        });
    });
});
