import { Policy, ReceiverLink, Constants as AmqpConstants } from 'amqp10';

import { ReceiveMode, LogFn } from './types';
import constants from './constants';
import { ErrorNames, throwError } from './errors';

const namespace = `${constants.debugNamespace}:credits`;

export class CreditManager {

    private _additionalCredits: number = 0;
    private _pendingSettles: Set<String> = new Set();
    private _link: ReceiverLink;

    static create(mode: ReceiveMode, initialCredit: number, threshold: number, basePolicy: Policy.PolicyBase, logFn: LogFn) {
        return new CreditManager(mode, initialCredit, threshold, basePolicy, logFn);
    }

    private constructor(private _mode: ReceiveMode, private _initialCredit: number, private _threshold: number, private _basePolicy: Policy.PolicyBase, private _logFn: LogFn) { }

    get policy(): Policy.PolicyBase {
        switch (this._mode) {
            case ReceiveMode.PeekLock:
                return <Policy.PolicyBase>Policy.Utils.Merge({
                    receiverLink: {
                        credit: (link: ReceiverLink, options?: any) => {
                            if (options && options.initial) {
                                this._logFn(namespace, `Initializing link with ${link.policy.creditQuantum} credits`);
                                link.addCredits(link.policy.creditQuantum);
                            }
                        },
                        creditQuantum: this._initialCredit,
                        attach: <any>{
                            rcvSettleMode: AmqpConstants.receiverSettleMode.settleOnDisposition
                        }
                    }
                }, this._basePolicy);
            case ReceiveMode.ReceiveAndDelete:
            default:
                return this._basePolicy;
        }
    }

    get receiverPolicy(): Policy.PolicyBase.ReceiverLink {
        return this.policy.receiverLink;
    }

    get pendingSettleCount(): number {
        return this._pendingSettles.size;
    }

    setReceiver(link: ReceiverLink) {
        this._link = link;
    }

    scheduleMessageSettle(lockToken: string) {
        // Only add a credit if this message wasn't already pending settle
        if (!this._pendingSettles.has(lockToken)) {
            this._additionalCredits++;
            this._pendingSettles.add(lockToken);
            this.refreshCredits();
        }
    }

    settleMessage(lockToken: string) {
        // Only add a credit if this message wasn't already pending settle
        if (!this._pendingSettles.delete(lockToken)) {
            this._additionalCredits++;
            this.refreshCredits();
        }
    }

    // A variant of the RefreshSettled credit function: https://github.com/noodlefrenzy/node-amqp10/blob/master/lib/policies/policy_utilities.js#L41
    refreshCredits() {
        // The link must be set by this point (it's an error otherwise)
        if (!this._link) {
            throwError(ErrorNames.Link.NotFound, 'Internal Error. No link assigned for credit manager');
        }

        // Don't try to send credits on a link that is not attached, since sending
        // a flow on a detached link can cause an amqp:session:unattached-handler
        // error: http://docs.oasis-open.org/amqp/core/v1.0/amqp-core-complete-v1.0.pdf#page=68
        if (this._link.state() !== 'attached') {
            this._logFn(namespace, `Not refreshing link ${this._link.name} credit because link is in state ${this._link.state()}`);
            return;
        }

        // Only add credits if we have credits to add and are below the threshold
        if (this._additionalCredits > 0 && this._link.linkCredit < this._threshold) {
            this._logFn(namespace, `Refreshing link ${this._link.name} credit by ${this._additionalCredits}. New credit total is ${this._link.linkCredit + this._additionalCredits}.`);
            this._link.addCredits(this._additionalCredits);
            this._additionalCredits = 0;
        }
    }
}
