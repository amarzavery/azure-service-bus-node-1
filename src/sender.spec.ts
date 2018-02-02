import { Sender } from './sender';
import { BrokeredMessage } from './brokeredMessage';
import { debug } from './utility';
import { mockAmqpClientManager, mockAmqpClient, mockSender } from './test/mocks';
import { ErrorNames } from './errors';
import { tickPromise } from './test/util';

describe('Sender', () => {

    let sender: Sender;
    let message: BrokeredMessage<any>;
    const timeout: number = 5000;
    let spy: jasmine.Spy;

    beforeEach(() => {
        spy = spyOn(mockAmqpClientManager, 'getAMQPClient').and.callThrough();
        spy = spyOn(mockAmqpClient, 'createSender').and.callThrough();

        message = new BrokeredMessage({ foo: 'bar' });
        message.to = 'to';
        sender = new Sender('myQueue', mockAmqpClientManager, debug);
    });

    afterEach(() => {
        sender.dispose();
    });

    it('logs attaches and detaches', async () => {
        const logSpy = jasmine.createSpy('log');
        const sender = new Sender('myQueue', mockAmqpClientManager, logSpy);
        await sender.send(message, timeout);

        const attachSpy = jasmine.createSpy('attach');
        const detachSpy = jasmine.createSpy('detach');
        sender.once(Sender.ATTACHED, attachSpy);
        sender.once(Sender.DETACHED, detachSpy);

        logSpy.calls.reset();
        expect(logSpy).not.toHaveBeenCalled();
        mockSender.emit('detached', 'foo');
        expect(logSpy).toHaveBeenCalledTimes(1);
        expect(detachSpy).toHaveBeenCalledTimes(1);
        mockSender.emit('attached');
        expect(logSpy).toHaveBeenCalledTimes(2);
        expect(attachSpy).toHaveBeenCalledTimes(1);
    });

    describe('#send', () => {
        it('sends an AMQP representation of the brokered message on the link', async () => {
            spyOn(mockSender, 'send').and.callThrough();

            await sender.send(message, timeout);

            expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
            expect(mockAmqpClient.createSender).toHaveBeenCalledTimes(1);
            expect(mockSender.send).toHaveBeenCalledWith(jasmine.objectContaining({
                properties: jasmine.objectContaining({
                    messageId: message.messageId,
                    to: message.to
                }),
                body: message.body
            }));
        });

        it('only creates the underlying AMQP link once', async () => {
            spyOn(mockSender, 'send').and.callThrough();

            await sender.send(message, timeout);
            await sender.send(message, timeout);

            expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
            expect(mockAmqpClient.createSender).toHaveBeenCalledTimes(1);
            expect(mockSender.send).toHaveBeenCalledTimes(2);
        });

        it('rejects the promise if the disposition on send is reject', async () => {
            spyOn(mockSender, 'send').and.returnValue(Promise.resolve({
                toDescribedType: (): any => ({ descriptor: 0x25 })
            }));

            try {
                await sender.send(message, timeout);
                fail();
            } catch (err) {
                expect(err.name).toEqual(ErrorNames.Send.Rejected);
                expect(mockAmqpClientManager.getAMQPClient).toHaveBeenCalledTimes(1);
                expect(mockAmqpClient.createSender).toHaveBeenCalledTimes(1);
                expect(mockSender.send).toHaveBeenCalledTimes(1);
            }
        });

        it('rejects the promise if sender is disposed', async () => {
            expect(sender.disposed).toBe(false);
            await sender.dispose();
            expect(sender.disposed).toBe(true);
            try {
                await sender.send(message, timeout);
                fail();
            } catch (err) {
                expect(err.name).toEqual(ErrorNames.Send.Disposed);
            }
        });

        it('rejects the promise if the send operation times out', done => {
            jasmine.clock().install();

            spyOn(mockSender, 'send').and.returnValue(new Promise(() => {}));

            sender.send(message, timeout)
                .then(fail)
                .catch((err: any) => {
                    jasmine.clock().uninstall();
                    expect(err.name).toEqual(ErrorNames.Send.Timeout);
                })
                .catch(fail)
                .then(done);

            tickPromise()
                .then(() => jasmine.clock().tick(timeout))
                .then(() => tickPromise());
        });

        const amqpErrorSymbols: [string, string][] = [
            ['amqp:internal-error', ErrorNames.Amqp.InternalError],
            ['amqp:not-found', ErrorNames.Amqp.NotFound],
            ['amqp:unauthorized-access', ErrorNames.Amqp.UnauthorizedAccess],
            ['amqp:decode-error', ErrorNames.Amqp.DecodeError],
            ['amqp:resource-limit-exceeded', ErrorNames.Amqp.ResourceLimitExceeded],
            ['amqp:not-allowed', ErrorNames.Amqp.NotAllowed],
            ['amqp:invalid-field', ErrorNames.Amqp.InvalidField],
            ['amqp:not-implemented', ErrorNames.Amqp.NotImplemented],
            ['amqp:resource-locked', ErrorNames.Amqp.ResourceLocked],
            ['amqp:precondition-failed', ErrorNames.Amqp.PreconditionFailed],
            ['amqp:resource-deleted', ErrorNames.Amqp.ResourceDeleted],
            ['amqp:illegal-state', ErrorNames.Amqp.IllegalState],
            ['amqp:frame-size-too-small', ErrorNames.Amqp.FrameSizeTooSmall],
            ['unknown', ErrorNames.Amqp.Unknown],
        ];

        for (const [symbol, name] of amqpErrorSymbols) {
            it(`rejects the promise if the send operation encounters an ${symbol} error`, async () => {
                const amqpError: any = new Error();
                amqpError.value = [{
                    typeName: 'symbol',
                    value: symbol
                }, {
                    typeName: 'string',
                    value: 'Error message'
                }];
                spyOn(mockSender, 'send').and.returnValue(Promise.reject(amqpError));

                try {
                    await sender.send(message, timeout);
                    fail();
                } catch (err) {
                    expect(err.name).toEqual(name);
                    expect(err.context.message).toBeDefined();
                }
            });
        }

        it('rejects the promise if the send operation encounters an unexpected error', async () => {
            spyOn(mockSender, 'send').and.returnValue(Promise.reject(new Error('boom')));

            try {
                await sender.send(message, timeout);
                fail();
            } catch (err) {
                expect(err.name).toEqual(ErrorNames.Internal.Unknown);
            }
        });
    });

    describe('#dispose', () => {
        it('stops sending if link still exists', async () => {
            expect(sender.disposed).toEqual(false);
            expect(await sender.canSend()).toEqual(true);
            let disposePromise = sender.dispose();
            expect(sender.disposed).toEqual(true);
            expect(await sender.canSend()).toEqual(false);
            await disposePromise;
            expect(sender.disposed).toEqual(true);
            expect(await sender.canSend()).toEqual(false);
        });

        it('can be called multiple times', async () => {
            expect(sender.disposed).toBe(false);
            expect(await sender.canSend()).toEqual(true);
            await sender.dispose();
            expect(sender.disposed).toBe(true);
            expect(await sender.canSend()).toEqual(false);
            await sender.dispose();
            expect(sender.disposed).toBe(true);
            expect(await sender.canSend()).toEqual(false);
        });

        it('can be called if the link is already detached for a different reason', async () => {
            expect(sender.disposed).toBe(false);
            expect(await sender.canSend()).toEqual(true);
            await sender.send(message, timeout);
            expect(sender.disposed).toBe(false);
            expect(await sender.canSend()).toEqual(true);
            mockSender.emit('detached', 'foo');
            spyOn(mockSender, 'canSend').and.returnValue(false);
            spyOn(mockSender, 'state').and.returnValue('detached');
            expect(sender.disposed).toBe(false);
            expect(await sender.canSend()).toEqual(false);
            await sender.dispose();
            expect(sender.disposed).toBe(true);
            expect(await sender.canSend()).toEqual(false);
        });
    });

    describe('#canSend', () => {
        it('returns false if setting up the sender encounters an error', async () => {
            mockAmqpClient.createSender.and.returnValue(Promise.reject(new Error('boom')));
            expect(await sender.canSend()).toBe(false);
        });
    });
});
