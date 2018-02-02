import {BrokeredMessage} from './brokeredMessage';
import {ProcessingState} from './types';

describe('BrokeredMessage', () => {

    let message: BrokeredMessage<any>;

    beforeEach(() => {
        message = new BrokeredMessage({});
    });

    it('is created with the provided message body', () => {
        const body = { foo: 'bar' };
        const message = new BrokeredMessage(body);

        expect(message.body).toEqual(body);
        expect(message.processingState).toEqual(ProcessingState.None);
    });

    it('allows all properties to be set', () => {
        const currentTime = new Date();
        const message = new BrokeredMessage({});
        message.contentType = 'string';
        message.correlationId = 'string';
        message.enqueuedSequenceNumber = 0;
        message.enqueuedTimeUtc = currentTime;
        message.label = 'string';
        message.messageId = 'string';
        message.partitionKey = 'string';
        message.replyTo = 'string';
        message.replyToSessionId = 'string';
        message.scheduledEnqueueTimeUtc = currentTime;
        message.sessionId = 'string';
        message.timeToLive = 0;
        message.to = 'string';

        expect(message.contentType).toEqual('string');
        expect(message.correlationId).toEqual('string');
        expect(message.enqueuedSequenceNumber).toEqual(0);
        expect(message.enqueuedTimeUtc).toEqual(currentTime);
        expect(message.label).toEqual('string');
        expect(message.messageId).toEqual('string');
        expect(message.partitionKey).toEqual('string');
        expect(message.replyTo).toEqual('string');
        expect(message.replyToSessionId).toEqual('string');
        expect(message.scheduledEnqueueTimeUtc).toEqual(currentTime);
        expect(message.sessionId).toEqual('string');
        expect(message.timeToLive).toEqual(0);
        expect(message.to).toEqual('string');
    });

    it('doesn\'t allow properties to be set with an invalid data type', () => {
        const message = new BrokeredMessage({});

        let error1: any;
        try {
            message.messageId = <any>0;
        } catch (err) {
            error1 = err;
        }

        let error2: any;
        try {
            message.enqueuedSequenceNumber = <any>'string';
        } catch (err) {
            error2 = err;
        }

        let error3: any;
        try {
            message.enqueuedTimeUtc = <any>'string';
        } catch (err) {
            error3 = err;
        }

        expect(error1 instanceof TypeError).toBe(true);
        expect(error2 instanceof TypeError).toBe(true);
        expect(error3 instanceof TypeError).toBe(true);
    });

    describe('#abandon', () => {
        it('does nothing', () => {
            expect(message.isSettled).toEqual(true);
            message.abandon();
            expect(message.isSettled).toEqual(true);
        });
    });

    describe('#complete', () => {
        it('does nothing', () => {
            expect(message.isSettled).toEqual(true);
            message.complete();
            expect(message.isSettled).toEqual(true);
        });
    });

    describe('#deadLetter', () => {
        it('does nothing', () => {
            expect(message.isSettled).toEqual(true);
            message.deadLetter();
            expect(message.isSettled).toEqual(true);
        });
    });

    describe('#renewLock', () => {
        it('does nothing', (done) => {
            message.renewLock()
                .catch(fail)
                .then(done);
        });
    });

    describe('#inspect', () => {
        it('shows all non-underscore properties', () => {
            const json = message.inspect();
            for (const key in json) {
                expect(key[0]).not.toEqual('_');
            }
        });
    });
});
