import * as uuid from 'node-uuid';
import { ReceiverLink } from 'amqp10';

import { BrokeredMessage } from './brokeredMessage';
import { AmqpRequestClient } from './amqpRequestClient';
import { AmqpMessage } from './internalTypes';
import constants from './constants';
import { ProcessingState, LogFn } from './types';
import { CreditManager } from './creditManager';
import { ErrorNames, throwError, buildError } from './errors';

const namespace = `${constants.debugNamespace}:message`;

export class InternalBrokeredMessage<T> extends BrokeredMessage<T> {

    private _processingState: ProcessingState;
    get processingState(): ProcessingState {
        return this._processingState;
    }

    get isSettled(): boolean {
        return this._processingState === ProcessingState.Settled;
    }

    constructor(private _amqpMessage: AmqpMessage<T>, frame: any, private _receiver: ReceiverLink, private _requestClient: AmqpRequestClient, private _creditManager: CreditManager, private _logFn: LogFn, settled: boolean = false) {
        super(_amqpMessage.body || null);

        this._processingState = settled ? ProcessingState.Settled : ProcessingState.Active;

        const amqpMessage = Object.assign({
            body: null,
            header: {},
            messageAnnotations: {},
            properties: {},
            applicationProperties: {}
        }, _amqpMessage);

        // Headers
        this._timeToLive = amqpMessage.header.ttl;
        this._deliveryCount = amqpMessage.header.deliveryCount;

        // Annotations
        this._enqueuedTimeUtc = amqpMessage.messageAnnotations['x-opt-enqueued-time'];
        this._enqueuedSequenceNumber = amqpMessage.messageAnnotations['x-opt-sequence-number'];
        this._partitionKey = amqpMessage.messageAnnotations['x-opt-partition-key'];
        this._lockedUntilUtc = amqpMessage.messageAnnotations['x-opt-locked-until'];

        // Properties
        this._messageId = amqpMessage.properties.messageId;
        this._to = amqpMessage.properties.to;
        this._label = amqpMessage.properties.subject;
        this._replyTo = amqpMessage.properties.replyTo;
        this._correlationId = amqpMessage.properties.correlationId;
        this._contentType = amqpMessage.properties.contentType;
        this._expiresAtUtc = amqpMessage.properties.absoluteExpiryTime;
        this._sessionId = amqpMessage.properties.groupId;
        this._replyToSessionId = amqpMessage.properties.replyToGroupId;
        this.properties = amqpMessage.applicationProperties;

        // Miscellaneous
        this._lockToken = uuid.unparse(frame.deliveryTag);
    }

    abandon(delayMs?: number): void {
        this._scheduleSettle(() => {
            this._logFn(namespace, `Settling message ${this.messageId} as abandoned`);
            this._receiver.modify(this._amqpMessage);
        }, delayMs);
    }

    complete(delayMs?: number): void {
        this._scheduleSettle(() => {
            this._logFn(namespace, `Settling message ${this.messageId} as completed`);
            this._receiver.accept(this._amqpMessage);
        }, delayMs);
    }

    deadLetter(delayMs?: number): void {
        this._scheduleSettle(() => {
            this._logFn(namespace, `Settling message ${this.messageId} as dead-lettered`);
            this._receiver.reject(this._amqpMessage);
        }, delayMs);
    }

    // defer(): void {}

    async renewLock(): Promise<void> {
        if (!this.isSettled && this.processingState !== ProcessingState.SettleFailed && this._requestClient) {
            try {
                await this._requestClient.renewLock(this.lockToken);
                this._logFn(namespace, `Renewed lock on message ${this.messageId}`);
            } catch (e) {
                if (e.name === ErrorNames.Internal.RequestTimeout) {
                    throwError(ErrorNames.Message.LockRenewalTimeout, `Timed out while renewing lock for message ${this.messageId}`, {
                        messageId: this.messageId
                    });
                } else if (e.name === ErrorNames.Internal.RequestFailure) {
                    throwError(ErrorNames.Message.LockRenewalFailure, `Encountered an error while renewing lock for message ${this.messageId}`, Object.assign({
                        messageId: this.messageId,
                    }, e.context));
                } else {
                    throwError(ErrorNames.Message.LockRenewalFailure, `Encountered an unknown error while renewing lock for message ${this.messageId}`, {
                        messageId: this.messageId,
                        errorMessage: e && e.message ? e.message : JSON.stringify(e)
                    });
                }
            }
        }
    }

    private _scheduleSettle(settle: () => void, delayMs?: number): void {
        const settleWrap = () => {
            try {
                // Proactively throw an exception if we try to acknowledge a message
                // on a link that isn't attached so that receivers can handle it
                // appropriately
                if (this._receiver.state() !== 'attached') {
                    throwError(ErrorNames.Message.SettleFailure, `Cannot settle message ${this.messageId} because its link is not attached`);
                }

                settle();

                // After settling, make sure the state is updated and we remove
                // all listeners
                this._processingState = ProcessingState.Settled;
                this.removeAllListeners();
            } catch (e) {
                // If settling failed, set the state accordingly
                this._processingState = ProcessingState.SettleFailed;
                this.emit(BrokeredMessage.SETTLE_ERROR, buildError(ErrorNames.Message.SettleFailure, `Encountered exception while settling message: ${e}`));
            } finally {
                // Make sure we always give the credit back so we don't starve
                // the receiver.
                this._creditManager.settleMessage(this.lockToken);
            }
        };

        // We have to have a credit manager
        if (!this._creditManager) {
            throwError(ErrorNames.Link.CreditManagerMissing, `Cannot find credit manager for link ${this._receiver.name}`, {
                linkName: this._receiver.name
            });
        }

        if (this.processingState !== ProcessingState.Active) {
            throwError(ErrorNames.Message.SettleFailure, `Cannot settle message ${this.messageId} because it is in processing state ${ProcessingState[this.processingState]}`, {
                messageId: this.messageId
            });
        }

        // Return the message now so that it doesn't count against our
        // concurrent limit
        if (delayMs > 0) {
            this._processingState = ProcessingState.Settling;
            this._creditManager.scheduleMessageSettle(this.lockToken);

            setTimeout(settleWrap, delayMs);
        } else {
            settleWrap();
        }
    }
}
