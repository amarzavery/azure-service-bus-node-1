import {MessageListenerImpl} from './messageListener';

describe('MessageListener', () => {

    let fakeCreditManager: any;
    let callback: any;

    beforeEach(() => {
        fakeCreditManager = {
            pendingSettleCount: 5
        };
        callback = jasmine.createSpy('callback');
    });

    describe('#dispose', () => {
        it('calls the cleanup callback if still subscribed', () => {
            const subscription = new MessageListenerImpl();
            subscription.initialize(callback, fakeCreditManager);
            expect(subscription.isListening).toEqual(true);
            subscription.dispose();
            expect(callback).toHaveBeenCalledTimes(1);
            expect(subscription.isListening).toEqual(false);
            subscription.dispose();
            expect(callback).toHaveBeenCalledTimes(1);
            expect(subscription.isListening).toEqual(false);
        });
    });

    describe('#pendingSettleCount', () => {
        it('returns 0 if the manager isn\'t initialized', () => {
            const subscription = new MessageListenerImpl();
            expect(subscription.pendingSettleCount).toEqual(0);
        });

        it('returns the corresponding value from the credit manager', () => {
            const subscription = new MessageListenerImpl();
            subscription.initialize(callback, fakeCreditManager);
            expect(subscription.pendingSettleCount).toEqual(fakeCreditManager.pendingSettleCount);
        });
    });
});
