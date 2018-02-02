import {EventEmitter} from 'events';
import * as uuid from 'node-uuid';
import * as createDebugNamespace from 'debug';

import constants from './constants';
import * as utility from './utility';
import {ProcessingState} from './types';

const debug = createDebugNamespace(`${constants.debugNamespace}:message`);

export interface MessageEventEmitter extends EventEmitter {
    on(event: 'settleError', listener: (err: Error) => void): this;
    on(event: string, listener: Function): this;

    emit(event: 'settleError', err: Error): boolean;
    emit(event: string, ...args: any[]): boolean;
}

/**
 * Representation of the data in a single message on service bus, as well as the
 * actions that can be performed with the message.
 */
export class BrokeredMessage<T> extends EventEmitter implements MessageEventEmitter {

    static SETTLE_ERROR = 'settleError';

    properties?: { [key: string]: any } = {};

    protected _contentType?: string;
    get contentType(): string { return this._contentType; }
    set contentType(value: string) {
        utility.verifyType(value, 'string');
        this._contentType = value;
    }

    protected _correlationId?: string;
    get correlationId(): string { return this._correlationId; }
    set correlationId(value: string) {
        utility.verifyType(value, 'string');
        this._correlationId = value;
    }

    // deadLetterSource: string;

    protected _deliveryCount?: number;
    get deliveryCount(): number { return this._deliveryCount; }

    protected _enqueuedSequenceNumber?: number;
    get enqueuedSequenceNumber(): number { return this._enqueuedSequenceNumber; }
    set enqueuedSequenceNumber(value: number) {
        utility.verifyType(value, 'number');
        this._enqueuedSequenceNumber = value;
    }

    protected _enqueuedTimeUtc?: Date;
    get enqueuedTimeUtc(): Date { return this._enqueuedTimeUtc; }
    set enqueuedTimeUtc(value: Date) {
        utility.verifyClass(value, Date, 'Date');
        this._enqueuedTimeUtc = value;
    }

    protected _expiresAtUtc?: Date;
    get expiresAtUtc(): Date { return this._expiresAtUtc; }

    // forcePersistence?: boolean;

    // isBodyConsumed?: boolean;

    protected _label?: string;
    get label(): string { return this._label; }
    set label(value: string) {
        utility.verifyType(value, 'string');
        this._label = value;
    }

    protected _lockedUntilUtc?: Date;
    get lockedUntilUtc(): Date { return this._lockedUntilUtc; }

    protected _lockToken?: string; // guid
    get lockToken(): string { return this._lockToken; }

    protected _messageId?: string = uuid.v4();
    get messageId(): string { return this._messageId; }
    set messageId(value: string) {
        utility.verifyType(value, 'string');
        this._messageId = value;
    }

    protected _partitionKey?: string;
    get partitionKey(): string { return this._partitionKey; }
    set partitionKey(value: string) {
        utility.verifyType(value, 'string');
        this._partitionKey = value;
    }

    protected _replyTo?: string;
    get replyTo(): string { return this._replyTo; }
    set replyTo(value: string) {
        utility.verifyType(value, 'string');
        this._replyTo = value;
    }

    protected _replyToSessionId?: string;
    get replyToSessionId(): string { return this._replyToSessionId; }
    set replyToSessionId(value: string) {
        utility.verifyType(value, 'string');
        this._replyToSessionId = value;
    }

    protected _scheduledEnqueueTimeUtc?: Date;
    get scheduledEnqueueTimeUtc(): Date { return this._scheduledEnqueueTimeUtc; }
    set scheduledEnqueueTimeUtc(value: Date) {
        utility.verifyClass(value, Date, 'Date');
        this._scheduledEnqueueTimeUtc = value;
    }

    protected _sequenceNumber?: number;
    get sequenceNumber(): number { return this._sequenceNumber; }

    protected _sessionId?: string;
    get sessionId(): string { return this._sessionId; }
    set sessionId(value: string) {
        utility.verifyType(value, 'string');
        this._sessionId = value;
    }

    // size?: number;

    // state?: Active | Deferred | Scheduled;

    protected _timeToLive?: number;
    get timeToLive(): number { return this._timeToLive; }
    set timeToLive(value: number) {
        utility.verifyType(value, 'number');
        this._timeToLive = value;
    }

    protected _to?: string;
    get to(): string { return this._to; }
    set to(value: string) {
        utility.verifyType(value, 'string');
        this._to = value;
    }

    // viaPartitionKey?: string;

    protected _body: T;
    get body(): T { return this._body; }

    get processingState(): ProcessingState { return ProcessingState.None; }

    constructor(body: T) {
        super();
        this._body = body;
    }

    /**
     * Abandons the lock on a peek-locked message
     *
     * @param delayMs   Optional delay before the message is actually abandoned
     *                  by the listener. During this delay period, the message
     *                  will not count against your concurrent message count,
     *                  but will also not be settled back to the service bus yet.
     */
    abandon(delayMs?: number): void {}

    /**
     * Completes the receive operation of a message and indicates that the
     * message should be marked as processed and deleted
     *
     * @param delayMs   Optional delay before the message is actually completed
     *                  by the listener. During this delay period, the message
     *                  will not count against your concurrent message count,
     *                  but will also not be settled back to the service bus yet.
     */
    complete(delayMs?: number): void {}

    /**
     * Moves the message to the dead letter queue
     *
     * @param delayMs   Optional delay before the message is actually dead-lettered
     *                  by the listener. During this delay period, the message
     *                  will not count against your concurrent message count,
     *                  but will also not be settled back to the service bus yet.
     */
    deadLetter(delayMs?: number): void {}

    // defer(): void {}

    get isSettled(): boolean { return true; }

    /**
     * Renews the lock on a message
     * @return {Promise<void>}
     */
    async renewLock(): Promise<void> {}

    inspect() {
        const result: any = {
            body: this.body,
            properties: this.properties,

            contentType: this.contentType,
            correlationId: this.correlationId,
            deliveryCount: this.deliveryCount,
            enqueuedSequenceNumber: this.enqueuedSequenceNumber,
            enqueuedTimeUtc: this.enqueuedTimeUtc,
            expiresAtUtc: this.expiresAtUtc,
            label: this.label,
            lockedUntilUtc: this.lockedUntilUtc,
            lockToken: this.lockToken,
            messageId: this.messageId,
            partitionKey: this.partitionKey,
            replyTo: this.replyTo,
            replyToSessionId: this.replyToSessionId,
            scheduledEnqueueTimeUtc: this.scheduledEnqueueTimeUtc,
            sequenceNumber: this.sequenceNumber,
            sessionId: this.sessionId,
            timeToLive: this.timeToLive,
            to: this.to
        };

        return result;
    }
}
