import { EventEmitter } from 'events';

import { ClientManager } from './clientManager';
import { Sender } from './sender';
import { Receiver } from './receiver';
import { BrokeredMessage } from './brokeredMessage';
import { Queue, MessageListener, OnMessageOptions, LogFn } from './types';
import { MessageListenerImpl } from './messageListener';

import constants from './constants';

export class QueueImpl extends EventEmitter implements Queue {

    private __sender: Sender;
    private get _sender(): Sender {
        if (!this.__sender) {
            this.__sender = new Sender(this.queueName, this._amqpClient, this._logFn);
            this.__sender.on(Sender.DETACHED, () => this.emit(Queue.SENDER_DETACHED));
            this.__sender.on(Sender.ATTACHED, () => this.emit(Queue.SENDER_REATTACHED));
        }

        return this.__sender;
    }

    private __receiver: Receiver;
    private get _receiver(): Receiver {
        if (!this.__receiver) {
            this.__receiver = new Receiver(this.queueName, this._amqpClient, this._logFn);
        }

        return this.__receiver;
    }

    private __deadLetterReceiver: Receiver;
    private get _deadLetterReceiver(): Receiver {
        if (!this.__deadLetterReceiver) {
            this.__deadLetterReceiver = new Receiver(`${this.queueName}/$DeadLetterQueue`, this._amqpClient, this._logFn);
        }

        return this.__deadLetterReceiver;
    }

    async canSend(): Promise<boolean> {
        return this._sender.canSend();
    }

    constructor(public queueName: string, private _amqpClient: ClientManager, private _logFn: LogFn) {
        super();
    }

    async send<T>(message: BrokeredMessage<T>, timeoutMs: number = constants.defaultSendTimeout): Promise<void> {
        await this._sender.send(message, timeoutMs);
    }

    onMessage<T>(listener: (message: BrokeredMessage<T>) => void | Promise<void>, options: OnMessageOptions = {}): MessageListener {
        return this._receiver.onMessage<T>(listener, options);
    }

    onDeadLetteredMessage<T>(listener: (message: BrokeredMessage<T>) => void | Promise<void>, options: OnMessageOptions = {}): MessageListener {
        return this._deadLetterReceiver.onMessage<T>(listener, options);
    }

    async receive<T>(timeoutMs?: number): Promise<BrokeredMessage<T>> {
        const messages = await this.receiveBatch<T>(1, timeoutMs);
        return messages[0];
    }

    async receiveBatch<T>(messageCount: number, timeoutMs?: number): Promise<BrokeredMessage<T>[]> {
        return this._receiver.receiveBatch<T>(messageCount, timeoutMs);
    }

    async disposeSender(): Promise<void> {
        await this._sender.dispose();
        this.__sender = null;
    }
}
