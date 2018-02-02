import { InternalBrokeredMessage } from './internalBrokeredMessage';
import { AmqpMessage } from './internalTypes';
import { CreditManager } from './creditManager';
import { ReceiveMode, ProcessingState } from './types';
import { mockAmqpClientManager, mockSender, mockReceiver } from './test/mocks';
import { buildError, ErrorNames } from './errors';
import { debug } from './utility';

describe('InternalBrokeredMessage', () => {

    let message: InternalBrokeredMessage<any>;
    let message2: InternalBrokeredMessage<any>;
    let amqpMessage: AmqpMessage<any>;
    let amqpMessage2: AmqpMessage<any>;
    let requestClient: any;
    let frame: any;

    beforeEach(() => {
        amqpMessage = {
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
            body: { foo: 'baz' },
            applicationProperties: {
                a: 'b',
                c: 'd'
            }
        };

        amqpMessage2 = {
            header: {
                ttl: 60000,
                deliveryCount: 3
            },
            properties: {
                messageId: 'messageId',
                to: 'to',
                subject: 'subject',
                replyTo: 'replyTo',
                correlationId: 'correlationId',
                contentType: 'contentType',
                absoluteExpiryTime: 'SHOULD NOT BE A STRING',
                groupId: 'groupId',
                replyToGroupId: 'replyToGroupId'
            },
            body: { foo: 'baz' },
            applicationProperties: {
                a: 'b',
                c: 'd'
            }
        };

        requestClient = {
            renewLock: jasmine.createSpy('renewLock').and.returnValue(Promise.resolve())
        };

        frame = {
            deliveryTag: Buffer.from([
                0x00, 0x01, 0x02, 0x03,
                0x04, 0x05,
                0x06, 0x07,
                0x08, 0x09,
                0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f
            ])
        };

        spyOn(mockReceiver, 'accept');
        spyOn(mockReceiver, 'reject');
        spyOn(mockReceiver, 'modify');

        const creditManager1 = CreditManager.create(ReceiveMode.PeekLock, 100, 50, <any>{}, debug);
        creditManager1.setReceiver(mockReceiver);
        const creditManager2 = CreditManager.create(ReceiveMode.ReceiveAndDelete, 100, 50, <any>{}, debug);
        creditManager2.setReceiver(mockReceiver);
        message = new InternalBrokeredMessage(amqpMessage, frame, mockReceiver, requestClient, creditManager1, debug, false);
        message2 = new InternalBrokeredMessage(amqpMessage, frame, mockReceiver, requestClient, creditManager2, debug, true);
    });

    it('initializes correctly', () => {
        expect(message.isSettled).toEqual(false);
        expect(message.lockToken).toEqual('00010203-0405-0607-0809-0a0b0c0d0e0f');

        expect(message.messageId).toEqual(amqpMessage.properties.messageId);
        expect(message.to).toEqual(amqpMessage.properties.to);
        expect(message.label).toEqual(amqpMessage.properties.subject);
        expect(message.replyTo).toEqual(amqpMessage.properties.replyTo);
        expect(message.correlationId).toEqual(amqpMessage.properties.correlationId);
        expect(message.contentType).toEqual(amqpMessage.properties.contentType);
        expect(message.expiresAtUtc).toEqual(amqpMessage.properties.absoluteExpiryTime);
        expect(message.sessionId).toEqual(amqpMessage.properties.groupId);
        expect(message.replyToSessionId).toEqual(amqpMessage.properties.replyToGroupId);

        expect(message.properties).toEqual(amqpMessage.applicationProperties);

        expect(message.timeToLive).toEqual(amqpMessage.header.ttl);
        expect(message.deliveryCount).toEqual(amqpMessage.header.deliveryCount);

        expect(message.enqueuedTimeUtc).toEqual(amqpMessage.messageAnnotations['x-opt-enqueued-time']);
        expect(message.enqueuedSequenceNumber).toEqual(amqpMessage.messageAnnotations['x-opt-sequence-number']);
        expect(message.partitionKey).toEqual(amqpMessage.messageAnnotations['x-opt-partition-key']);
        expect(message.lockedUntilUtc).toEqual(amqpMessage.messageAnnotations['x-opt-locked-until']);
    });

    it('allows the source message to contain invalid data types', () => {
        const message = new InternalBrokeredMessage(amqpMessage2, frame, mockReceiver, requestClient, CreditManager.create(ReceiveMode.ReceiveAndDelete, 100, 50, <any>{}, debug), debug, false);

        expect(message.expiresAtUtc).toEqual(amqpMessage2.properties.absoluteExpiryTime);
    });

    describe('#abandon', () => {
        it('calls modify on the receiver using the AMQP message if unsettled', () => {
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);
            message.abandon();
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);
            expect(mockReceiver.modify).toHaveBeenCalledWith(amqpMessage);
        });

        it('throws if the message is already settled', () => {
            expect(message2.isSettled).toEqual(true);
            expect(message2.processingState).toEqual(ProcessingState.Settled);

            expect(message2.abandon).toThrowError();

            try {
                message2.abandon();
            } catch (e) {
                expect(e.name).toEqual('ServiceBus:Message:SettleError');
                expect(message2.isSettled).toEqual(true);
                expect(message2.processingState).toEqual(ProcessingState.Settled);
                expect(mockReceiver.modify).not.toHaveBeenCalled();
            }
        });

        it('optionally settles the message after the provided delay', () => {
            jasmine.clock().install();
            const delay = 5000;

            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);

            message.abandon(delay);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(delay - 1);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(1);
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);

            jasmine.clock().uninstall();
        });

        it('requires the creditManager to be set', () => {
            const message = new InternalBrokeredMessage(amqpMessage, frame, mockReceiver, requestClient, null, debug, false);
            expect(message.abandon.bind(message)).toThrowError();
            try {
                message.abandon();
            } catch (e) {
                expect(e.name).toEqual(ErrorNames.Link.CreditManagerMissing);
            }
        });

        it('emits a settleError if settling the message fails', () => {
            (mockReceiver.modify as jasmine.Spy).and.throwError('boom!');
            const onSpy = jasmine.createSpy('on');
            message.on('settleError', onSpy);

            message.abandon();
            expect(onSpy).toHaveBeenCalledTimes(1);
            expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
            expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                name: ErrorNames.Message.SettleFailure
            }));
        });

        it('emits a settleError if settling the link is not attached', () => {
            spyOn(mockReceiver, 'state').and.returnValue('detached');
            const onSpy = jasmine.createSpy('on');
            message.on('settleError', onSpy);

            message.abandon();
            expect(onSpy).toHaveBeenCalledTimes(1);
            expect(onSpy).toHaveBeenCalledWith(jasmine.any(Error));
            expect(onSpy).toHaveBeenCalledWith(jasmine.objectContaining({
                name: ErrorNames.Message.SettleFailure
            }));
        });
    });

    describe('#complete', () => {
        it('calls accept on the receiver using the AMQP message if unsettled', () => {
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);
            message.complete();
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);
            expect(mockReceiver.accept).toHaveBeenCalledWith(amqpMessage);
        });

        it('throws if the message is already settled', () => {
            expect(message2.isSettled).toEqual(true);
            expect(message2.processingState).toEqual(ProcessingState.Settled);

            expect(message2.abandon).toThrowError();

            try {
                message2.complete();
            } catch (e) {
                expect(e.name).toEqual('ServiceBus:Message:SettleError');
                expect(message2.isSettled).toEqual(true);
                expect(message2.processingState).toEqual(ProcessingState.Settled);
                expect(mockReceiver.modify).not.toHaveBeenCalled();
            }
        });

        it('optionally settles the message after the provided delay', () => {
            jasmine.clock().install();
            const delay = 5000;

            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);

            message.complete(delay);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(delay - 1);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(1);
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);

            jasmine.clock().uninstall();
        });
    });

    describe('#deadLetter', () => {
        it('calls reject on the receiver using the AMQP message if unsettled', () => {
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);
            message.deadLetter();
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);
            expect(mockReceiver.reject).toHaveBeenCalledWith(amqpMessage);
        });

        it('does nothing if the message is already settled', () => {
            expect(message2.isSettled).toEqual(true);
            expect(message2.processingState).toEqual(ProcessingState.Settled);

            expect(message2.abandon).toThrowError();

            try {
                message2.deadLetter();
            } catch (e) {
                expect(e.name).toEqual('ServiceBus:Message:SettleError');
                expect(message2.isSettled).toEqual(true);
                expect(message2.processingState).toEqual(ProcessingState.Settled);
                expect(mockReceiver.modify).not.toHaveBeenCalled();
            }
        });

        it('optionally settles the message after the provided delay', () => {
            jasmine.clock().install();
            const delay = 5000;

            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Active);

            message.deadLetter(delay);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(delay - 1);
            expect(message.isSettled).toEqual(false);
            expect(message.processingState).toEqual(ProcessingState.Settling);

            jasmine.clock().tick(1);
            expect(message.isSettled).toEqual(true);
            expect(message.processingState).toEqual(ProcessingState.Settled);

            jasmine.clock().uninstall();
        });
    });

    describe('#renewLock', () => {
        it('renews the lock on the AMQP message if unsettled', (done) => {
            expect(message.isSettled).toEqual(false);
            message.renewLock()
                .then(() => {
                    expect(message.isSettled).toEqual(false);
                    expect(requestClient.renewLock).toHaveBeenCalledWith(message.lockToken);
                })
                .catch(fail)
                .then(done);
        });

        it('does nothing if the message is already settled', (done) => {
            expect(message2.isSettled).toEqual(true);
            message2.renewLock()
                .then(() => {
                    expect(message2.isSettled).toEqual(true);
                    expect(requestClient.renewLock).not.toHaveBeenCalled();
                })
                .catch(fail)
                .then(done);
        });

        it('throws a lock renewal timeout error if the underlying call times out', (done) => {
            (requestClient.renewLock as jasmine.Spy).and.returnValue(Promise.reject(buildError(ErrorNames.Internal.RequestTimeout, 'blah')));
            message.renewLock()
                .then(() => fail)
                .catch(err => {
                    expect(err.name).toEqual(ErrorNames.Message.LockRenewalTimeout);
                })
                .catch(fail)
                .then(done);
        });

        it('throws a lock renewal error if the underlying call fails with a request failure', (done) => {
            (requestClient.renewLock as jasmine.Spy).and.returnValue(Promise.reject(buildError(ErrorNames.Internal.RequestFailure, 'blah')));
            message.renewLock()
                .then(() => fail)
                .catch(err => {
                    expect(err.name).toEqual(ErrorNames.Message.LockRenewalFailure);
                })
                .catch(fail)
                .then(done);
        });

        it('throws a lock renewal error if the underlying call fails with an unexpected error', (done) => {
            (requestClient.renewLock as jasmine.Spy).and.returnValue(Promise.reject(new Error('boom!')));
            message.renewLock()
                .then(() => fail)
                .catch(err => {
                    expect(err.name).toEqual(ErrorNames.Message.LockRenewalFailure);
                })
                .catch(fail)
                .then(done);
        });

        it('throws a lock renewal error if the underlying call fails with something other than an Error', (done) => {
            (requestClient.renewLock as jasmine.Spy).and.returnValue(Promise.reject('boom!'));
            message.renewLock()
                .then(() => fail)
                .catch(err => {
                    expect(err.name).toEqual(ErrorNames.Message.LockRenewalFailure);
                })
                .catch(fail)
                .then(done);
        });
    });
});
