import { EventEmitter } from 'events';
import { SenderLink } from 'amqp10';
import { DeliveryState } from 'amqp10/typings/lib/types/delivery_state';
import Session = require('amqp10/typings/lib/session');
import * as uuid from 'node-uuid';

import { ClientManager } from './clientManager';
import { BrokeredMessage } from './brokeredMessage';
import { AmqpMessage } from './internalTypes';
import { setIfDefined } from './utility';
import constants from './constants';
import { LogFn } from './types';
import { ErrorNames, throwError, buildError, wrapError } from './errors';

const namespace = `${constants.debugNamespace}:sender`;

interface SenderRef {
    dispose: () => void;
    sender: Promise<[SenderLink, Session]>;
}

export class Sender extends EventEmitter {

    public static DETACHED = 'detached';
    public static ATTACHED = 'attached';

    private _disposed: boolean = false;
    get disposed(): boolean {
        return this._disposed;
    }

    private __sender: SenderRef;
    private get _sender(): SenderRef {
        if (!this.__sender) {
            this._logFn(namespace, `Creating link to send messages on path ${this._path}`);
            const amqpClient = this._amqpClient.getAMQPClient();
            this.__sender = {
                dispose: amqpClient.dispose,
                sender: (async () => {
                    const client = await amqpClient.client;
                    const session: Session = await (client as any).createSession();
                    const sender: SenderLink = await (client as any).createSender(this._path, {}, session);
                    sender.on('detached', (info: any) => {
                        this._logFn(namespace, `Sender link detached on path ${this._path}`);
                        this.emit(Sender.DETACHED);
                    });

                    sender.on('attached', (info: any) => {
                        this._logFn(namespace, `Sender link attached on path ${this._path}`);
                        this.emit(Sender.ATTACHED);
                    });

                    this._logFn(namespace, `Finished creating link to send messages on path ${this._path}`);
                    return [sender, session] as [SenderLink, Session];
                })()
            };
        }

        return this.__sender;
    }

    private static readonly AMQP_REJECTED_DESCRIPTOR = 0x25;

    constructor(private _path: string, private _amqpClient: ClientManager, private _logFn: LogFn) {
        super();
    }

    async canSend(): Promise<boolean> {
        if (this.disposed) {
            return false;
        }

        return this._sender.sender
            .then(([sender]) => sender.state() === 'attached')
            .catch(() => false);
    }

    async send<T>(message: BrokeredMessage<T>, timeoutMs: number): Promise<void> {
        if (this._disposed) {
            this._logFn(namespace, `Message ${message.messageId} not sent. Sender is disposed.`);
            throwError(ErrorNames.Send.Disposed, 'Message not sent. Sender is disposed');
        }

        this._logFn(namespace, `Sending message ${message.messageId} on path ${this._path}`);
        const [sender] = await this._sender.sender;

        // The underlying AMQP library doesn't support a timeout on send,
        // causing messages that are waiting for delivery/acknowledgement
        // to potentiall hang forever. We address this by having the send
        // operation race with a timer. If the timer completes first, the
        // send will fail with a timeout. If the send completes first, the
        // timer will get cleared out.
        // 
        // TODO: push the timeout down to amqp10 once supported
        const result = await new Promise<DeliveryState>((resolve, reject) => {
            const sendTimeout = setTimeout(() => {
                reject(buildError(
                    ErrorNames.Send.Timeout,
                    `Sender timed out. Failed to send message ${message.messageId}`
                ));
            }, timeoutMs);

            sender.send(Sender._brokeredMessageToAmqpMessage(message))
                .then(result => {
                    clearTimeout(sendTimeout);
                    resolve(result);
                })
                .catch(err => {
                    clearTimeout(sendTimeout);
                    reject(wrapError(err));
                });
        });

        // If the message is rejected, reject the promise
        if (result.toDescribedType().descriptor === Sender.AMQP_REJECTED_DESCRIPTOR) {
            this._logFn(namespace, `Message ${message.messageId} rejected by Service Bus`);
            throwError(ErrorNames.Send.Rejected, 'Message send rejected');
        }

        this._logFn(namespace, `Message ${message.messageId} sent successfully`);
    }

    async dispose(): Promise<void> {
        if (!this._disposed) {
            this._disposed = true;
            const disposeSender = this._sender.dispose;
            const senderPromise = this._sender.sender;

            try {
                const [sender, session] = await senderPromise;
                (session as any).end({ dispose: true });
                session.removeAllListeners();
                sender.removeAllListeners();
                this.removeAllListeners();
                disposeSender();
                this.__sender = null;
            } catch (err) { }
        }
    }

    private static _brokeredMessageToAmqpMessage<T>(message: BrokeredMessage<T>): AmqpMessage<T> {
        const amqpMessage: AmqpMessage<T> = {
            body: message.body,
            messageAnnotations: {},
            properties: {
                messageId: message.messageId
            },
            applicationProperties: message.properties
        };

        setIfDefined(amqpMessage.messageAnnotations, 'x-opt-partition-key', message.partitionKey);
        setIfDefined(amqpMessage.messageAnnotations, 'x-opt-enqueued-time', message.enqueuedTimeUtc);
        setIfDefined(amqpMessage.messageAnnotations, 'x-opt-sequence-number', message.enqueuedSequenceNumber);
        setIfDefined(amqpMessage.messageAnnotations, 'x-opt-scheduled-enqueue-time', message.scheduledEnqueueTimeUtc);

        setIfDefined(amqpMessage.properties, 'to', message.to);
        setIfDefined(amqpMessage.properties, 'subject', message.label);
        setIfDefined(amqpMessage.properties, 'replyTo', message.replyTo);
        setIfDefined(amqpMessage.properties, 'correlationId', message.correlationId);
        setIfDefined(amqpMessage.properties, 'contentType', message.contentType);
        setIfDefined(amqpMessage.properties, 'groupId', message.sessionId);
        setIfDefined(amqpMessage.properties, 'replyToGroupId', message.replyToSessionId);

        return amqpMessage;
    }
}
