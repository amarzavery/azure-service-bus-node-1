import { EventEmitter } from 'events';
import { Policy, Constants as AMQPConstants } from 'amqp10';
import { Receiver } from './receiver';
import { BrokeredMessage } from './brokeredMessage';
import { InternalBrokeredMessage } from './internalBrokeredMessage';
import constants from './constants';
import { AmqpMessage } from './internalTypes';
import { AmqpRequestClient } from './amqpRequestClient';
import { ReceiveMode } from './types';
import { buildError, ErrorNames } from './errors';
import { CreditManager } from './creditManager';
import { debug } from './utility';

import { mockAmqpClientManager, mockAmqpClient, mockReceiver, mockSession } from './test/mocks';
import { tickPromise } from './test/util';

describe('Receiver', () => {

    let requestClient: any;
    let path = 'myQueue';

    let message1: AmqpMessage<any>;
    let message2: AmqpMessage<any>;

    let receiver: Receiver;

    beforeEach(() => {
        message1 = {
            header: {
                ttl: 60000,
                deliveryCount: 3
            },
            messageAnnotations: {
                'x-opt-enqueued-time': new Date(),
                'x-opt-sequence-number': 2,
                'x-opt-partition-key': 'partitionKey',
                'x-opt-locked-until': new Date()
            },
            properties: {
                messageId: 'messageId',
                to: 'to',
                subject: 'subject',
                replyTo: 'replyTo',
                correlationId: 'correlationId',
                contentType: 'contentType',
                absoluteExpiryTime: new Date(),
                groupId: 'groupId',
                replyToGroupId: 'replyToGroupId'
            },
            body: { foo: 'bar' },
            applicationProperties: {
                a: 'b',
                c: 'd'
            }
        };

        message2 = {
            header: {
                ttl: 15000,
                deliveryCount: 1
            },
            messageAnnotations: {
                'x-opt-enqueued-time': new Date(),
                'x-opt-sequence-number': 2,
                'x-opt-partition-key': 'partitionKey',
                'x-opt-locked-until': new Date()
            },
            properties: {
                messageId: 'messageId2',
                to: 'to',
                subject: 'subject',
                replyTo: 'replyTo',
                correlationId: 'correlationId',
                contentType: 'contentType',
                absoluteExpiryTime: new Date(),
                groupId: 'groupId',
                replyToGroupId: 'replyToGroupId'
            },
            body: { foo: 'baz' },
            applicationProperties: {
                a: 'b',
                c: 'd'
            }
        };

        requestClient = new EventEmitter();
        requestClient.renewLock = jasmine.createSpy('renewLock').and.returnValue(Promise.resolve());
        requestClient.dispose = jasmine.createSpy('dispose');

        spyOn(AmqpRequestClient, 'create').and.returnValue(Promise.resolve(requestClient));

        spyOn(mockAmqpClientManager, 'getAMQPClient').and.callThrough();
        spyOn(mockAmqpClient, 'createReceiver').and.callThrough();
        spyOn(mockReceiver, 'addCredits').and.callThrough();
        spyOn(mockReceiver, 'detach').and.callThrough();
        spyOn(mockSession, 'end').and.callThrough();

        receiver = new Receiver(path, mockAmqpClientManager, debug);
    });

    describe('#onMessage', () => {
        it('calls the provided listener with a BrokeredMessage for each message received', done => {
            let messageCount = 0;
            let lastMessage: BrokeredMessage<any>;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);
                lastMessage = message;

                messageCount++;
            });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    expect(lastMessage.isSettled).toBe(true);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('handles if parts of the message are missing', done => {
            let messageCount = 0;
            let lastMessage: BrokeredMessage<any>;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(undefined);
                lastMessage = message;

                messageCount++;
            });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage({});
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    expect(lastMessage.isSettled).toBe(true);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('auto-settles the message if the mode is set to ReceiveAndDelete', done => {
            let messageCount = 0;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(true);
                expect(message.messageId).toEqual(message1.properties.messageId);

                messageCount++;
            }, {
                    receiveMode: ReceiveMode.ReceiveAndDelete
                });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('allows multiple messages to be received', done => {
            let messageCount = 0;
            const maxConcurrentCalls = 100;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                switch (messageCount) {
                    case 0:
                        expect(message.messageId).toEqual(message1.properties.messageId);
                        break;
                    case 1:
                        expect(message.messageId).toEqual(message2.properties.messageId);
                        break;
                }

                messageCount++;
            }, {
                    maxConcurrentCalls
                });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                    mockReceiver.sendMessage(message2);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(2);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('leaves the message unsettled after the handler returns if autoComplete is false', done => {
            let messageCount = 0;
            let lastMessage: BrokeredMessage<any>;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);
                lastMessage = message;

                messageCount++;
            }, {
                    autoComplete: false
                });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    expect(lastMessage.isSettled).toBe(false);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('renews the lock on the message within the auto-renew timeout', done => {
            jasmine.clock().install();

            let fakeTime = Date.now();
            spyOn(Date, 'now').and.callFake(() => fakeTime);

            let messageCount = 0;
            let lastMessage: BrokeredMessage<any>;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);
                lastMessage = message;

                messageCount++;
            }, {
                    autoRenewTimeout: constants.serviceBusDeliveryTimeout * 2,
                    autoComplete: false
                });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    jasmine.clock().tick(constants.serviceBusDeliveryTimeout);
                    fakeTime += constants.serviceBusDeliveryTimeout;
                })
                .then(() => tickPromise(5))
                .then(() => {
                    // First renewal
                    expect(requestClient.renewLock).toHaveBeenCalledTimes(1);
                    expect(requestClient.renewLock).toHaveBeenCalledWith(lastMessage.lockToken);
                    requestClient.renewLock.calls.reset();

                    jasmine.clock().tick(constants.serviceBusDeliveryTimeout);
                    fakeTime += constants.serviceBusDeliveryTimeout;
                })
                .then(() => tickPromise(5))
                .then(() => {
                    // Second renewal
                    expect(requestClient.renewLock).toHaveBeenCalledTimes(1);
                    expect(requestClient.renewLock).toHaveBeenCalledWith(lastMessage.lockToken);
                    requestClient.renewLock.calls.reset();

                    jasmine.clock().tick(constants.serviceBusDeliveryTimeout);
                    fakeTime += constants.serviceBusDeliveryTimeout;
                })
                .then(() => tickPromise(5))
                .then(() => {
                    // After the timeout expires, no more renewals should happen
                    expect(requestClient.renewLock).not.toHaveBeenCalled();
                    expect(messageCount).toEqual(1);
                    expect(lastMessage.isSettled).toBe(false);
                    subscription.dispose();
                    jasmine.clock().uninstall();
                })
                .catch(fail)
                .then(done);
        });

        it('renews the lock on the message within the auto-renew timeout', done => {
            (requestClient.renewLock as jasmine.Spy).and.returnValue(Promise.reject(buildError(ErrorNames.Internal.RequestFailure, 'sdlfkj')));

            jasmine.clock().install();

            let fakeTime = Date.now();
            spyOn(Date, 'now').and.callFake(() => fakeTime);

            let messageCount = 0;
            let lastMessage: BrokeredMessage<any>;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);
                lastMessage = message;

                messageCount++;
            }, {
                    autoRenewTimeout: constants.serviceBusDeliveryTimeout * 2,
                    autoComplete: false
                });

            const onSpy = jasmine.createSpy('on');
            subscription.on('receiverError', onSpy);

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(onSpy).not.toHaveBeenCalled();
                    jasmine.clock().tick(constants.serviceBusDeliveryTimeout);
                    fakeTime += constants.serviceBusDeliveryTimeout;
                })
                .then(() => tickPromise(5))
                .then(() => {
                    // First renewal
                    expect(requestClient.renewLock).toHaveBeenCalledTimes(1);
                    expect(requestClient.renewLock).toHaveBeenCalledWith(lastMessage.lockToken);
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Message.LockRenewalFailure
                    }));
                    subscription.dispose();
                    jasmine.clock().uninstall();
                })
                .catch(fail)
                .then(done);
        });

        it('does not try to renew a message lock after the message has been settled', done => {
            jasmine.clock().install();

            let fakeTime = Date.now();
            spyOn(Date, 'now').and.callFake(() => fakeTime);

            let messageCount = 0;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);

                messageCount++;
            });

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    jasmine.clock().tick(constants.serviceBusDeliveryTimeout);
                    fakeTime += constants.serviceBusDeliveryTimeout;
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    subscription.dispose();
                    jasmine.clock().uninstall();
                })
                .catch(fail)
                .then(done);
        });

        it('emits a detached event on the subscription when one is received', done => {
            let messageCount = 0;

            const subscription = receiver.onMessage<any>(message => {
                expect(message.isSettled).toBe(false);
                expect(message.messageId).toEqual(message1.properties.messageId);

                messageCount++;
            });

            const detachInfo = { foo: 'bar' };
            const detachSpy = jasmine.createSpy('detached');
            subscription.on('detached', detachSpy);

            tickPromise(5)
                .then(() => {
                    expect(AmqpRequestClient.create).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            creditQuantum: constants.maxConcurrentCalls,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.settleOnDisposition
                            })
                        }),
                        jasmine.anything()
                    );

                    mockReceiver.sendMessage(message1);
                    mockReceiver.sendDetach(detachInfo);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageCount).toEqual(1);
                    expect(detachSpy).toHaveBeenCalledTimes(1);
                    expect(detachSpy).toHaveBeenCalledWith(detachInfo);
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('forwards errors from messages', (done) => {
            const messageSpy = jasmine.createSpy('onMessage');
            const subscription = receiver.onMessage<any>(messageSpy, {
                autoComplete: false
            });

            const onSpy = jasmine.createSpy('on');
            subscription.on('receiverError', onSpy);

            tickPromise(5)
                .then(() => {
                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).not.toHaveBeenCalled();
                    messageSpy.calls.first().args[0].emit('settleError', buildError(ErrorNames.Message.SettleFailure, 'sdfjslkfj'));
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Message.SettleFailure
                    }));
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });

        it('forwards errors from the request client', (done) => {
            const messageSpy = jasmine.createSpy('onMessage');
            const subscription = receiver.onMessage<any>(messageSpy, {
                autoComplete: false
            });

            const onSpy = jasmine.createSpy('on');
            subscription.on('receiverError', onSpy);

            tickPromise(5)
                .then(() => {
                    expect(onSpy).not.toHaveBeenCalled();

                    requestClient.emit('requestClientError', new Error('boom!'));
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Internal.Unknown
                    }));

                    onSpy.calls.reset();

                    requestClient.emit('requestClientError', null);
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Internal.Unknown
                    }));

                    subscription.dispose();
                })
                .then(() => tickPromise(5))
                .catch(fail)
                .then(done);
        });

        it('emits an initial attach event an forwards subsequent attach events on the receiver', (done) => {
            const messageSpy = jasmine.createSpy('onMessage');
            const subscription = receiver.onMessage<any>(messageSpy, {
                autoComplete: false
            });

            const onSpy = jasmine.createSpy('on');
            subscription.on('attached', onSpy);

            tickPromise(5)
                .then(() => {
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    mockReceiver.emit('attached');
                    expect(onSpy).toHaveBeenCalledTimes(2);

                    subscription.dispose();
                })
                .then(() => tickPromise(5))
                .catch(fail)
                .then(done);
        });

        it('tries to reconnect after a fixed timeout if there is a failure during initialization', (done) => {
            (mockAmqpClient.createReceiver as jasmine.Spy).and.returnValue(Promise.reject(new Error('boom!')));

            const messageSpy = jasmine.createSpy('onMessage');
            const subscription = receiver.onMessage<any>(messageSpy);

            const onSpy = jasmine.createSpy('on receiverError');
            const attachedSpy = jasmine.createSpy('on attached');
            subscription.on('receiverError', onSpy);
            subscription.on('attached', attachedSpy);

            jasmine.clock().install();

            tickPromise(5)
                .then(() => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    // No attached event if the receiver was never created
                    expect(attachedSpy).not.toHaveBeenCalled();
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Link.Detach
                    }));

                    jasmine.clock().tick(constants.reattachInterval);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(2);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(2);

                    jasmine.clock().uninstall();

                    subscription.dispose();
                })
                .then(() => tickPromise(5))
                .catch(fail)
                .then(done);
        });

        it('emits a recieverError if processing a message fails', (done) => {
            spyOn(CreditManager.prototype, 'refreshCredits').and.throwError('boom!');
            const messageSpy = jasmine.createSpy('onMessage');
            const subscription = receiver.onMessage<any>(messageSpy, {
                autoComplete: false
            });

            const onSpy = jasmine.createSpy('on');
            subscription.on('receiverError', onSpy);

            tickPromise(5)
                .then(() => {
                    expect(onSpy).not.toHaveBeenCalled();
                    mockReceiver.sendMessage(message1);
                })
                .then(() => tickPromise(5))
                .then(() => {
                    expect(messageSpy).not.toHaveBeenCalled();
                    expect(onSpy).toHaveBeenCalledTimes(1);
                    expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
                    expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                        name: ErrorNames.Internal.Unknown
                    }));
                    subscription.dispose();
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('#receiveBatch', () => {
        it('creates a receiver in auto-settle mode with credits for the number of messages to receive', done => {
            const messageCount = 2;
            receiver.receiveBatch(messageCount)
                .then(messages => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            credit: Policy.Utils.CreditPolicies.DoNotRefresh,
                            creditQuantum: 0,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                            })
                        }),
                        jasmine.anything()
                    );
                    expect(mockReceiver.addCredits).toHaveBeenCalledTimes(1);
                    expect(mockReceiver.addCredits).toHaveBeenCalledWith(messageCount);
                    expect(mockSession.end).toHaveBeenCalledTimes(1);

                    expect(messages.length).toEqual(2);
                    expect(messages).toEqual(jasmine.arrayContaining([
                        jasmine.objectContaining({
                            messageId: message1.properties.messageId
                        }),
                        jasmine.objectContaining({
                            messageId: message2.properties.messageId
                        }),
                    ]) as any);
                })
                .catch(fail)
                .then(done);

            tickPromise(10)
                .then(() => {
                    mockReceiver.sendMessage(message1);
                    mockReceiver.sendMessage(message2);
                });
        });

        it('handles if parts of the message are missing', done => {
            const messageCount = 1;
            receiver.receiveBatch(messageCount)
                .then(messages => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            credit: Policy.Utils.CreditPolicies.DoNotRefresh,
                            creditQuantum: 0,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                            })
                        }),
                        jasmine.anything()
                    );
                    expect(mockReceiver.addCredits).toHaveBeenCalledTimes(1);
                    expect(mockReceiver.addCredits).toHaveBeenCalledWith(messageCount);
                    expect(mockSession.end).toHaveBeenCalledTimes(1);

                    expect(messages.length).toEqual(messageCount);
                    expect(messages).toEqual(jasmine.arrayContaining([
                        jasmine.objectContaining({
                            messageId: undefined
                        })
                    ]) as any);
                })
                .catch(fail)
                .then(done);

            tickPromise(10)
                .then(() => {
                    mockReceiver.sendMessage({});
                });
        });

        it('returns if the timeout is reached, even if the number of messages requested has not been fulfilled', done => {
            jasmine.clock().install();

            const messageCount = 5;
            const timeout = 15000;
            receiver.receiveBatch(messageCount, timeout)
                .then(messages => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            credit: Policy.Utils.CreditPolicies.DoNotRefresh,
                            creditQuantum: 0,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                            })
                        }),
                        jasmine.anything()
                    );
                    expect(mockReceiver.addCredits).toHaveBeenCalledTimes(1);
                    expect(mockReceiver.addCredits).toHaveBeenCalledWith(messageCount);
                    expect(mockSession.end).toHaveBeenCalledTimes(1);

                    expect(messages.length).toEqual(2);
                    expect(messages).toEqual(jasmine.arrayContaining([
                        jasmine.objectContaining({
                            messageId: message1.properties.messageId
                        }),
                        jasmine.objectContaining({
                            messageId: message2.properties.messageId
                        }),
                    ]) as any);

                    jasmine.clock().uninstall();
                })
                .catch(fail)
                .then(done);

            tickPromise(10)
                .then(() => {
                    mockReceiver.sendMessage(message1);
                    mockReceiver.sendMessage(message2);
                })
                .then(() => tickPromise(10))
                .then(() => jasmine.clock().tick(timeout));
        });

        it('rejects if the receiver is detached during receiving', done => {
            const error = new Error('boom goes the dynamite');
            const messageCount = 2;
            receiver.receiveBatch(messageCount)
                .then(() => Promise.reject(new Error('did not fail when expected to')))
                .catch(err => {
                    expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledTimes(1);
                    expect(mockAmqpClient.createReceiver).toHaveBeenCalledWith(
                        path,
                        jasmine.objectContaining({
                            credit: Policy.Utils.CreditPolicies.DoNotRefresh,
                            creditQuantum: 0,
                            attach: jasmine.objectContaining({
                                rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                            })
                        }),
                        jasmine.anything()
                    );
                    expect(mockReceiver.addCredits).toHaveBeenCalledTimes(1);
                    expect(mockReceiver.addCredits).toHaveBeenCalledWith(messageCount);
                    expect(mockSession.end).toHaveBeenCalledTimes(1);

                    expect(err).toEqual(error);
                })
                .catch(fail)
                .then(done);

            tickPromise(10)
                .then(() => {
                    mockReceiver.sendDetach(error);
                });
        });
    });
});
