import {Constants as AmqpConstants} from 'amqp10';
import * as uuid from 'node-uuid';
import { debug } from './utility';

import {CreditManager} from './creditManager';
import {ReceiveMode} from './types';

describe('CreditManager', () => {

    let link: any;
    let basePolicy: any;

    beforeEach(() => {
        link = {
            addCredits: jasmine.createSpy('addCredits'),
            state: jasmine.createSpy('state').and.returnValue('attached')
        };

        basePolicy = { receiverLink: { some: 'policy' } };
    });

    describe('#policy', () => {
        it('returns the base policy when in ReceiveMode.ReceiveAndDelete', () => {
            const basePolicy: any = { receiverLink: { some: 'policy' } };
            const manager = CreditManager.create(ReceiveMode.ReceiveAndDelete, 100, 50, basePolicy, debug);

            expect(manager.policy).toEqual(basePolicy);
            expect(manager.receiverPolicy).toEqual(basePolicy.receiverLink);
            expect(manager.pendingSettleCount).toEqual(0);
        });

        it('sets the settle mode to peek lock and updates the credit function when set to ReceiveMode.PeekLock', () => {
            const basePolicy: any = { receiverLink: { some: 'policy' } };
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);

            expect(manager.policy).toEqual({ receiverLink: jasmine.objectContaining(basePolicy.receiverLink) } as any);
            expect(manager.receiverPolicy).toEqual(jasmine.objectContaining(basePolicy.receiverLink));
            expect(manager.receiverPolicy.credit).toEqual(jasmine.any(Function));
            expect((<any>manager.receiverPolicy.attach).rcvSettleMode).toEqual(AmqpConstants.receiverSettleMode.settleOnDisposition);
            expect(manager.pendingSettleCount).toEqual(0);
        });
    });

    describe('credit update function', () => {
        it('adds the creditQuantum on startup', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.policy = manager.receiverPolicy;
            link.linkCredit = 0;

            manager.receiverPolicy.credit(link, { initial: true });
            expect(link.addCredits).toHaveBeenCalledTimes(1);
            expect(link.addCredits).toHaveBeenCalledWith(100);
        });

        it('does nothing when not called on startup', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.policy = manager.receiverPolicy;
            link.linkCredit = 0;

            manager.receiverPolicy.credit(link, {});
            manager.receiverPolicy.credit(link);
            expect(link.addCredits).not.toHaveBeenCalled();
        });
    });

    describe('#refreshCredits', () => {
        it('does nothing when no messages have been returned', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 0;
            manager.setReceiver(link);

            manager.refreshCredits();
            expect(link.addCredits).not.toHaveBeenCalled();
        });

        it('does nothing when the threshold hasn\'t been reached', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 99;
            manager.setReceiver(link);

            for (let i = 0; i < 100; i++) {
                manager.scheduleMessageSettle(uuid.v4());
            }

            manager.refreshCredits();
            expect(link.addCredits).not.toHaveBeenCalled();
        });

        it('does nothing if the link isn\'t attached', () => {
            link.state = jasmine.createSpy('state').and.returnValue('detached');
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            manager.settleMessage(uuid.v4());

            link.linkCredit = 49;
            manager.refreshCredits();
            expect(link.addCredits).not.toHaveBeenCalled();
        });

        it('adds a credit for each returned message when below the threshold', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            const returnedMessages = 14;
            const settledMessages = 10;
            const returnedAndSettledMessages = 5;

            for (let i = 0; i < returnedMessages; i++) {
                manager.scheduleMessageSettle(uuid.v4());
            }

            for (let i = 0; i < settledMessages; i++) {
                manager.settleMessage(uuid.v4());
            }

            for (let i = 0; i < returnedAndSettledMessages; i++) {
                const id = uuid.v4();
                manager.scheduleMessageSettle(id);
                manager.settleMessage(id);
            }

            link.linkCredit = 49;
            manager.refreshCredits();
            expect(link.addCredits).toHaveBeenCalledTimes(1);
            expect(link.addCredits).toHaveBeenCalledWith(returnedMessages + settledMessages + returnedAndSettledMessages);
        });

        it('does not add any credits when at the threshold', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 50;
            manager.setReceiver(link);

            const returnedMessages = 14;
            for (let i = 0; i < returnedMessages; i++) {
                manager.scheduleMessageSettle(uuid.v4());
            }

            manager.refreshCredits();
            expect(link.addCredits).not.toHaveBeenCalled();
        });

        it('throws an exception when there is no link provided', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            expect(manager.refreshCredits.bind(this)).toThrowError(Error);
        });
    });

    describe('#scheduleMessageSettle', () => {
        it('increments the pending settle count the first time it is called with a message', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            for (let i = 1; i <= 10; i++) {
                manager.scheduleMessageSettle(uuid.v4());
                expect(manager.pendingSettleCount).toEqual(i);
            }
        });

        it('refreshes the appropriate number of credits when below the threshold', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            const initialMessageCount = 10;
            for (let i = 1; i <= initialMessageCount; i++) {
                manager.scheduleMessageSettle(uuid.v4());
                expect(manager.pendingSettleCount).toEqual(i);
            }

            link.linkCredit = 49;
            manager.scheduleMessageSettle(uuid.v4());
            expect(link.addCredits).toHaveBeenCalledTimes(1);
            expect(link.addCredits).toHaveBeenCalledWith(initialMessageCount + 1);
        });

        it('does not increment the pending settle count for subsequent calls with the same message', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            for (let i = 1; i <= 10; i++) {
                const id = uuid.v4();
                manager.scheduleMessageSettle(id);
                manager.scheduleMessageSettle(id);
                manager.scheduleMessageSettle(id);
                manager.scheduleMessageSettle(id);
                manager.scheduleMessageSettle(id);
                expect(manager.pendingSettleCount).toEqual(i);
            }
        });
    });

    describe('#settleMessage', () => {
        it('decrements the pending settle count if previously called with scheduleMessageSettle()', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            const ids: string[] = [];
            for (let i = 0; i < 10; i++) {
                ids.push(uuid.v4());
            }

            for (const [index, id] of ids.entries()) {
                manager.scheduleMessageSettle(id);
                expect(manager.pendingSettleCount).toEqual(index + 1);
            }

            for (const [index, id] of ids.entries()) {
                manager.settleMessage(id);
                expect(manager.pendingSettleCount).toEqual(ids.length - (index + 1));
            }
        });

        it('refreshes the appropriate number of credits when below the threshold', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            const ids: string[] = [];
            for (let i = 0; i < 10; i++) {
                ids.push(uuid.v4());
            }

            for (const [index, id] of ids.entries()) {
                manager.scheduleMessageSettle(id);
                expect(manager.pendingSettleCount).toEqual(index + 1);
            }

            for (const [index, id] of ids.entries()) {
                manager.settleMessage(id);
                expect(manager.pendingSettleCount).toEqual(ids.length - (index + 1));
            }

            link.linkCredit = 49;
            manager.settleMessage(uuid.v4());
            expect(link.addCredits).toHaveBeenCalledTimes(1);
            expect(link.addCredits).toHaveBeenCalledWith(ids.length + 1);
        });

        it('does not decrement the pending settle count if not previously called with scheduleMessageSettle()', () => {
            const manager = CreditManager.create(ReceiveMode.PeekLock, 100, 50, basePolicy, debug);
            link.linkCredit = 100;
            manager.setReceiver(link);

            const ids: string[] = [];
            const ids2: string[] = [];
            for (let i = 0; i < 10; i++) {
                ids.push(uuid.v4());
                ids2.push(uuid.v4());
            }

            for (const [index, id] of ids.entries()) {
                manager.scheduleMessageSettle(id);
                expect(manager.pendingSettleCount).toEqual(index + 1);
            }

            for (const [index, id] of ids2.entries()) {
                manager.settleMessage(id);
                expect(manager.pendingSettleCount).toEqual(ids.length);
            }
        });
    });
});
