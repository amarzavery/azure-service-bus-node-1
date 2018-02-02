import { QueueImpl } from './queue';
import { Sender } from './sender';
import { Receiver } from './receiver';
import { BrokeredMessage } from './brokeredMessage';
import { Queue, MessageListener } from './types';
import { debug } from './utility';

import { mockAmqpClientManager } from './test/mocks';
import { tickPromise } from './test/util';

describe('Queue', () => {

    let queue: Queue;

    const message1 = new BrokeredMessage({ message: 1 });
    const message2 = new BrokeredMessage({ message: 2 });
    const timeout = 15000;
    const sendTimeout = 50120;

    beforeAll(() => {
        queue = new QueueImpl('myQueue', mockAmqpClientManager, debug);
    });

    describe('#send', () => {
        it('calls send on the Sender', done => {
            spyOn(Sender.prototype, 'send').and.callThrough();
            queue.send(message1)
                .then(() => queue.send(message2, sendTimeout))
                .then(() => {
                    expect(Sender.prototype.send).toHaveBeenCalledWith(message1, 15000);
                    expect(Sender.prototype.send).toHaveBeenCalledWith(message2, sendTimeout);
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('events', () => {
        it('emits detached and reattached events from the underlying sender', () => {
            const attachSpy = jasmine.createSpy('attach');
            const detachSpy = jasmine.createSpy('detach');
            queue.on(Queue.SENDER_DETACHED, detachSpy);
            queue.on(Queue.SENDER_REATTACHED, attachSpy);

            (queue as any)._sender.emit(Sender.DETACHED);
            expect(detachSpy).toHaveBeenCalledTimes(1);
            (queue as any)._sender.emit(Sender.ATTACHED);
            expect(attachSpy).toHaveBeenCalledTimes(1);
        });
    });

    describe('#onMessage', () => {
        it('creates a receiver and listens for messages on it', done => {
            spyOn(Receiver.prototype, 'onMessage').and.callThrough();
            const listener = (message: any) => { };

            let subscription: MessageListener;

            try {
                subscription = queue.onMessage(listener);
                expect(Receiver.prototype.onMessage).toHaveBeenCalledWith(listener, {});
                expect(subscription.isListening).toEqual(true);
            } finally {
                if (subscription) {
                    subscription.dispose();
                }
            }

            // Clear out promises
            tickPromise(5).then(done);
        });
    });

    describe('#onDeadLetteredMessage', () => {
        it('creates a receiver and listens for messages on it', done => {
            spyOn(Receiver.prototype, 'onMessage').and.callThrough();
            const listener = (message: any) => { };

            let subscription: MessageListener;

            try {
                subscription = queue.onDeadLetteredMessage(listener);
                expect(Receiver.prototype.onMessage).toHaveBeenCalledWith(listener, {});
                expect(subscription.isListening).toEqual(true);
            } finally {
                if (subscription) {
                    subscription.dispose();
                }
            }

            // Clear out promises
            tickPromise(5).then(done);
        });
    });

    describe('#receive', () => {
        it('calls receiveBatch on the receiver with a single message', done => {
            spyOn(Receiver.prototype, 'receiveBatch').and.returnValue(Promise.resolve([message1]));

            queue.receive(timeout)
                .then(message => {
                    expect(Receiver.prototype.receiveBatch).toHaveBeenCalledTimes(1);
                    expect(Receiver.prototype.receiveBatch).toHaveBeenCalledWith(1, timeout);
                    expect(message).toEqual(message1);
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('#receiveBatch', () => {
        it('calls receiveBatch on the receiver with the provided number of messages', done => {
            spyOn(Receiver.prototype, 'receiveBatch').and.returnValue(Promise.resolve([message1, message2]));

            const messageCount = 5;
            queue.receiveBatch(messageCount, timeout)
                .then(messages => {
                    expect(Receiver.prototype.receiveBatch).toHaveBeenCalledTimes(1);
                    expect(Receiver.prototype.receiveBatch).toHaveBeenCalledWith(messageCount, timeout);
                    expect(messages.length).toEqual(2);
                    expect(messages).toEqual(jasmine.arrayContaining([message1, message2]));
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('#disposeSender', () => {
        it('disposes sender', done => {
            spyOn(Sender.prototype, 'dispose');

            queue.disposeSender()
                .then(() => {
                    expect(Sender.prototype.dispose).toHaveBeenCalledTimes(1);
                })
                .catch(fail)
                .then(done);
        });
    });

    describe('#canSend', () => {
        it('passes through to the underlying sender', async () => {
            spyOn(Sender.prototype, 'canSend').and.returnValue(Promise.resolve(true));
            expect(await queue.canSend()).toBe(true);
        });
    });
});
