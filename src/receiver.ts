import * as uuid from 'node-uuid';
import { EventEmitter } from 'events';
import { ReceiverLink, Policy, Constants as AMQPConstants } from '@azure-iot/amqp10';
import Session = require('@azure-iot/amqp10/lib/session');

import { ClientManager } from './clientManager';
import { BrokeredMessage } from './brokeredMessage';
import { InternalBrokeredMessage } from './internalBrokeredMessage';
import { AmqpRequestClient } from './amqpRequestClient';
import { OnMessageOptions, ReceiveMode, MessageListener, ProcessingState, LogFn } from './types';
import { AmqpMessage } from './internalTypes';
import { MessageListenerImpl } from './messageListener';
import constants from './constants';
import { CreditManager } from './creditManager';
import { ErrorNames, buildError, wrapError } from './errors';

const namespace = `${constants.debugNamespace}:receiver`;

export interface RenewTimer {
    message: BrokeredMessage<any>;
    timer: NodeJS.Timer;
}

export class Receiver {

    static RECEIVER_ERROR = 'receiverError';
    static DETACHED = 'detached';
    static ATTACHED = 'attached';
    static MANAGEMENT_LINK_ATTACHED = 'managementLinkAttached';
    static MANAGEMENT_LINK_DETACHED = 'managementLinkDetached';

    private static get basePolicy(): any {
        return {
            receiverLink: {
                name: uuid.v4(),
                attach: {
                    target: {
                        address: 'localhost'
                    },
                    rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                }
            }
        };
    }

    private _getReceiver(policy: any): { dispose: () => void, receiver: Promise<[ReceiverLink, Session]> } {
        this._logFn(namespace, `Creating link to receive messages on path ${this._path}`);
        const amqpClient = this._amqpClient.getAMQPClient();
        return {
            dispose: amqpClient.dispose,
            receiver: (async () => {
                const client = await amqpClient.client;
                const session: Session = await (client as any).createSession();
                const receiver: ReceiverLink = await (client as any).createReceiver(this._path, policy, session);
                this._logFn(namespace, `Finished creating link to receive messages on path ${this._path}`);
                return [receiver, session] as [ReceiverLink, Session];
            })()
        };
    }

    constructor(private _path: string, private _amqpClient: ClientManager, private _logFn: LogFn) { }

    onMessage<T>(listener: (message: BrokeredMessage<T>) => void | Promise<void>, config: OnMessageOptions = {}): MessageListener {
        config = Object.assign({}, <OnMessageOptions>{
            receiveMode: ReceiveMode.PeekLock,
            autoComplete: true,
            autoRenewTimeout: constants.autoRenewTimeout,
            maxConcurrentCalls: constants.maxConcurrentCalls
        }, config);

        const messageListener = new MessageListenerImpl();
        this._connectReceiver(messageListener, listener, config);
        return messageListener;
    }

    async receiveBatch<T>(messageCount: number, timeoutMs?: number): Promise<BrokeredMessage<T>[]> {
        timeoutMs = timeoutMs != null ? timeoutMs : constants.serviceBusServerTimeout;

        const policy = Policy.merge({
            receiverLink: {
                credit: Policy.Utils.CreditPolicies.DoNotRefresh, // Once the batch has been sent, we are done
                creditQuantum: 0, // Don't let any messages through initially. We'll control it manually
                attach: <any>{
                    rcvSettleMode: AMQPConstants.receiverSettleMode.autoSettle
                }
            }
        }, Receiver.basePolicy).receiverLink;


        const { dispose: disposeReceiver, receiver: receiverPromise } = this._getReceiver(policy);
        const [receiver, session] = await receiverPromise;

        return new Promise<BrokeredMessage<T>[]>((resolve, reject) => {
            const receiveTimeout = setTimeout(() => {
                dispose();
                resolve(messages);
            }, timeoutMs);

            const dispose = () => {
                clearTimeout(receiveTimeout);
                (session as any).end({ dispose: true });
                session.removeAllListeners();
                receiver.removeAllListeners();
                disposeReceiver();
            };

            const messages: BrokeredMessage<T>[] = [];
            receiver.on('message', (message: AmqpMessage<T>, frame: any) => {
                messages.push(new InternalBrokeredMessage<T>(
                    message,
                    frame,
                    receiver,
                    null,
                    null,
                    this._logFn,
                    true
                ));

                if (messages.length >= messageCount) {
                    dispose();
                    resolve(messages);
                }
            });

            receiver.on('detached', (info: any) => {
                dispose();
                reject(info);
            });

            // Tell the service bus how many messages it is allowed to send us.
            //
            // Per the AMQP 1.0 spec (http://www.amqp.org/sites/amqp.org/files/amqp.pdf?p=44)
            // there is an option to 'drain' the queue/subscription when sending
            // a flow of credits. In this mode, the sender is supposed to send
            // whatever messages it has, up to the number of messages we specify.
            //
            // The benefit of this approach is that if the sender runs out of
            // messages before the credits have been consumed, it will send a
            // flow indicating that it is drained. This would allow us to end
            // the call before the timeout is reached, even if the messaging
            // entity doesn't provide as many messages as we asked for.
            //
            // Unfortunately, Service Bus does not appear to support the drain
            // option yet. Once it does, our code here would look something like
            // this. We would still need to be able to watch for the flow in
            // response, but I'm not going to try to drive that requirement to
            // the node-amqp10 library until I know we might be able to use it:
            //
            //      receiver.totalCredits = messageCount;
            //      receiver.flow({
            //          drain: true
            //      });
            //
            receiver.addCredits(messageCount);
        });
    }

    private _connectReceiver<T>(messageListener: MessageListenerImpl, listenerFn: (message: BrokeredMessage<T>) => void | Promise<void>, config: OnMessageOptions): void {
        const creditManager = CreditManager.create(
            config.receiveMode,
            config.maxConcurrentCalls,
            Math.ceil(config.maxConcurrentCalls / 2),
            Receiver.basePolicy,
            this._logFn
        );

        const { dispose: disposeReceiver, receiver: receiverPromise } = this._getReceiver(creditManager.receiverPolicy);
        const requestClientPromise = AmqpRequestClient.create(this._path, this._amqpClient, this._logFn);

        const renewalTimers: Map<string, RenewTimer> = new Map();
        const renewalTimeouts: Map<string, number> = new Map();

        const cleanupCallback = async () => {
            for (const timer of renewalTimers.values()) {
                timer.message.removeAllListeners();
                clearTimeout(timer.timer);
            }

            renewalTimers.clear();
            renewalTimeouts.clear();

            await Promise.all([
                requestClientPromise.then(requestClient => requestClient.dispose()),
                receiverPromise.then(([receiver, session]) => {
                    (session as any).end({ dispose: true });
                    session.removeAllListeners();
                    receiver.removeAllListeners();
                    disposeReceiver();
                })
            ]).catch(err => { });
        };

        messageListener.initialize(
            cleanupCallback,
            creditManager
        );

        let onLinkError = (err?: any) => {
            messageListener.emit(Receiver.RECEIVER_ERROR, buildError(ErrorNames.Link.Detach, `Link ${creditManager.receiverPolicy.name} detached on path ${this._path}`, {
                linkName: creditManager.receiverPolicy.name,
                path: this._path,
                err: err
            }));
        };

        (async () => {
            const [receiver] = await receiverPromise;
            // Emit an attached event because we will miss the first one sent by
            // the receiver
            messageListener.emit(Receiver.ATTACHED);

            const requestClient = await requestClientPromise;

            creditManager.setReceiver(receiver);

            requestClient.on(AmqpRequestClient.REQUEST_CLIENT_ERROR, (err) => {
                messageListener.emit(Receiver.RECEIVER_ERROR, wrapError(err));
            });

            requestClient.on(AmqpRequestClient.LINK_ATTACHED, (name: string, path: string, isSender: boolean) => {
                messageListener.emit(Receiver.MANAGEMENT_LINK_ATTACHED, name, path, isSender);
            });

            requestClient.on(AmqpRequestClient.LINK_DETACHED, (name: string, path: string, isSender: boolean, info: any) => {
                messageListener.emit(Receiver.MANAGEMENT_LINK_DETACHED, name, path, isSender, info);
            });

            receiver.on('message', async (message: AmqpMessage<T>, frame: any) => {
                // Just in case we've fallen below our threshold, add back any
                // extra credits we might have sitting around
                try {
                    creditManager.refreshCredits();

                    const settled: boolean = config.receiveMode !== ReceiveMode.PeekLock;
                    const brokeredMessage = new InternalBrokeredMessage<T>(
                        message,
                        frame,
                        receiver,
                        requestClient,
                        creditManager,
                        this._logFn,
                        settled
                    );
                    this._logFn(namespace, `Received message ${brokeredMessage.messageId} on path ${this._path}`);
                    this._logFn(namespace, brokeredMessage.isSettled ?
                        `Auto-settling message ${brokeredMessage.messageId} based on configuration` :
                        `Leaving message ${brokeredMessage.messageId} unsettled based on configuration`
                    );

                    // Set up a listener on the message for errors (the message will
                    // clear the listener for us when it settles)
                    brokeredMessage.on(BrokeredMessage.SETTLE_ERROR, (err: Error) => {
                        messageListener.emit(Receiver.RECEIVER_ERROR, wrapError(err));
                    });

                    this._scheduleRenewal(brokeredMessage, config, renewalTimers, renewalTimeouts, messageListener);

                    const succeeded = await Promise.resolve(listenerFn(brokeredMessage))
                        .then(() => true)
                        .catch(() => false);

                    // Reject any message where the handler throws an exception
                    if (!succeeded) {
                        this._logFn(namespace, `Abandoning message ${brokeredMessage.messageId} due to exception while handling`);
                        brokeredMessage.abandon();
                    }

                    // If we made it through processing the message successfully and
                    // autoComplete is turned on, complete the message
                    if (!brokeredMessage.isSettled && config.autoComplete && succeeded) {
                        this._logFn(namespace, `Automatically completing message ${brokeredMessage.messageId} after successful handling due to configuration`);
                        brokeredMessage.complete();
                    }
                } catch (e) {
                    messageListener.emit(Receiver.RECEIVER_ERROR, wrapError(e));
                }
            });

            receiver.on('attached', () => {
                messageListener.emit(Receiver.ATTACHED);
            });

            receiver.on('detached', (info: any) => {
                messageListener.emit(Receiver.DETACHED, info);
                onLinkError(info);
            });
        })().catch((error) => {
            this._logFn(namespace, `Unhandled error from link on path ${this._path}: ${error}`);
            onLinkError(error);
            messageListener.disposeInternals();

            // We put our reattach on a timer to avoid a storm of reconnects
            // and to prevent stack overflows
            setTimeout(() => {
                this._logFn(namespace, `Attempting to reattach link on path ${this._path}`);
                this._connectReceiver(messageListener, listenerFn, config);
            }, constants.reattachInterval);
        });
    }

    private _scheduleRenewal<T>(message: BrokeredMessage<T>, config: OnMessageOptions, renewalTimers: Map<string, RenewTimer>, renewalTimeouts: Map<string, number>, subscription: MessageListener) {
        // Clear things out if the message is already settled
        if (message.isSettled || message.processingState === ProcessingState.SettleFailed) {
            this._logFn(namespace, `Not setting and/or clearing renewal timer for message ${message.messageId} because it is settled`);
            renewalTimers.delete(message.lockToken);
            renewalTimeouts.delete(message.lockToken);
            return;
        }

        // Set the timeout for renewal if not already set
        if (!renewalTimeouts.has(message.lockToken)) {
            renewalTimeouts.set(message.lockToken, Date.now() + config.autoRenewTimeout);
        }

        const timeUntilRenewal = constants.serviceBusDeliveryTimeout * constants.renewThreshold;

        // Only set the timer if we are still in the auto-renewal window
        if (renewalTimeouts.get(message.lockToken) > Date.now() + timeUntilRenewal) {
            renewalTimers.set(message.lockToken, {
                message,
                timer: setTimeout(async () => {
                    try {
                        if (!message.isSettled) {
                            await message.renewLock();
                            this._scheduleRenewal(message, config, renewalTimers, renewalTimeouts, subscription);
                        } else {
                            this._logFn(namespace, `Not renewing lock on message ${message.messageId} because it has been settled`);
                            renewalTimers.delete(message.lockToken);
                            renewalTimeouts.delete(message.lockToken);
                        }
                    } catch (e) {
                        subscription.emit('receiverError', wrapError(e));
                    }
                }, timeUntilRenewal)
            });
            this._logFn(namespace, `Scheduled message ${message.messageId} for renewal at ${new Date(Date.now() + timeUntilRenewal).toUTCString()}`);
        } else {
            // Otherwise, clear our data out
            this._logFn(namespace, `Auto-renewal timeout for message ${message.messageId} has been exceeded. The message will not be renewed any more`);
            renewalTimers.delete(message.lockToken);
            renewalTimeouts.delete(message.lockToken);
        }
    }
}
