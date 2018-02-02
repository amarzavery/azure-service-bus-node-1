import { EventEmitter } from 'events';

import { ClientManager } from './clientManager';
import { Sender } from './sender';
import { Receiver } from './receiver';
import { BrokeredMessage } from './brokeredMessage';
import { Topic, MessageListener, OnMessageOptions, LogFn } from './types';
import { MessageListenerImpl } from './messageListener';
import constants from './constants';

export class TopicImpl extends EventEmitter implements Topic {

    private __sender: Sender;
    private get _sender(): Sender {
        if (!this.__sender) {
            this.__sender = new Sender(this.topicName, this._amqpClient, this._logFn);
            this.__sender.on(Sender.DETACHED, () => this.emit(Topic.SENDER_DETACHED));
            this.__sender.on(Sender.ATTACHED, () => this.emit(Topic.SENDER_REATTACHED));
        }

        return this.__sender;
    }

    private _receivers: Map<string, Receiver> = new Map();
    private _deadLetterReceivers: Map<string, Receiver> = new Map();

    private _getReceiver(subscriptionName: string): Receiver {
        if (!this._receivers.has(subscriptionName)) {
            this._receivers.set(subscriptionName, new Receiver(`${this.topicName}/Subscriptions/${subscriptionName}`, this._amqpClient, this._logFn));
        }

        return this._receivers.get(subscriptionName);
    }

    private _getDeadLetterReceiver(subscriptionName: string): Receiver {
        if (!this._deadLetterReceivers.has(subscriptionName)) {
            this._deadLetterReceivers.set(subscriptionName, new Receiver(`${this.topicName}/Subscriptions/${subscriptionName}/$DeadLetterQueue`, this._amqpClient, this._logFn));
        }

        return this._deadLetterReceivers.get(subscriptionName);
    }

    async canSend(): Promise<boolean> {
        return this._sender.canSend();
    }

    constructor(public topicName: string, private _amqpClient: ClientManager, private _logFn: LogFn) {
        super();
    }

    async send<T>(message: BrokeredMessage<T>, timeoutMs: number = constants.defaultSendTimeout): Promise<void> {
        await this._sender.send(message, timeoutMs);
    }

    onMessage<T>(subscriptionName: string, listener: (message: BrokeredMessage<T>) => void | Promise<void>, options: OnMessageOptions = {}): MessageListener {
        return this._getReceiver(subscriptionName).onMessage(listener, options);
    }

    onDeadLetteredMessage<T>(subscriptionName: string, listener: (message: BrokeredMessage<T>) => void | Promise<void>, options: OnMessageOptions = {}): MessageListener {
        return this._getDeadLetterReceiver(subscriptionName).onMessage(listener, options);
    }

    async receive<T>(subscriptionName: string, timeoutMs?: number): Promise<BrokeredMessage<T>> {
        const messages = await this.receiveBatch<T>(subscriptionName, 1, timeoutMs);
        return messages[0];
    }

    async receiveBatch<T>(subscriptionName: string, messageCount: number, timeoutMs?: number): Promise<BrokeredMessage<T>[]> {
        return this._getReceiver(subscriptionName).receiveBatch<T>(messageCount, timeoutMs);
    }

    async disposeSender(): Promise<void> {
        await this._sender.dispose();
        this.__sender = null;
    }
}
