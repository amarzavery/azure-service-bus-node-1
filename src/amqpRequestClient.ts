import { EventEmitter } from 'events';
import * as path from 'path';
import * as HttpStatus from 'http-status';
import { Client as AMQPClient, SenderLink, ReceiverLink, Constants, DescribedType } from '@azure-iot/amqp10';
import Session = require('@azure-iot/amqp10/lib/session');
const ForcedType = require('@azure-iot/amqp10/lib/types/forced_type');
const AMQPArray = require('@azure-iot/amqp10/lib/types/amqp_composites').Array;
import * as uuid from 'node-uuid';
import { LogFn } from './types';

import constants from './constants';
import { ClientManager, CreateSenderOptions, CreateReceiverOptions, CreateLinkGroupOptions, LinkGroup } from './clientManager';
import { AmqpMessage, AmqpRequest, AmqpResponse } from './internalTypes';
import { ErrorNames, throwError, buildError } from './errors';

const namespace = `${constants.debugNamespace}:request-client`;

export interface AmqpRequestClientEventEmitter extends EventEmitter {
    on(event: 'requestClientError', listener: (err: Error) => void): this;
    on(event: 'detached', listener: (err: Error) => void): this;
    on(event: string, listener: Function): this;

    emit(event: 'requestClientError', err: Error): boolean;
    emit(event: 'detached', err: Error): boolean;
    emit(event: string, ...args: any[]): boolean;
}

export class AmqpRequestClient extends EventEmitter {

    static REQUEST_CLIENT_ERROR = 'requestClientError';
    static LINK_ATTACHED = 'linkAttached';
    static LINK_DETACHED = 'linkDetached';

    private _isListening: boolean = false;

    /**
     * Create an Amqp request/response link pair for performing management actions
     *
     * @param  {string}                     topicName        The topic to manage
     * @param  {string}                     subscriptionName The subscription to manage
     * @param  {AMQPClient}                 amqpClient       The amqp client to use for connecting
     * @return {Promise<AmqpRequestClient>}                  The management client
     */
    static async create(entityPath: string, amqpClient: ClientManager, logFn: LogFn): Promise<AmqpRequestClient> {
        const { client: clientPromise, dispose } = amqpClient.getAMQPClient(2);
        const client = await clientPromise;
        const session = await (client as any).createSession();
        const [sender, receiver] = await Promise.all([
            AmqpRequestClient._createSender(client, session, entityPath),
            AmqpRequestClient._createReceiver(client, session, entityPath)
        ]);
        return new AmqpRequestClient(entityPath, session, sender, receiver, dispose, logFn);
    }

    private static async _createSender(client: AMQPClient, session: any, entityPath: string): Promise<SenderLink> {
        const senderLinkName = `requestSender$${uuid.v4()}`;
        return (client as any).createSender(AmqpRequestClient._getManagementPath(entityPath), <any>{ // TODO: Update to official typings
            name: senderLinkName,
            attach: {
                name: senderLinkName,
                source: {
                    address: senderLinkName
                }
            },
            encoder: null
        }, session);
    }

    private static async _createReceiver(client: AMQPClient, session: any, entityPath: string): Promise<ReceiverLink> {
        const receiverLinkName = `responseReceiver$${uuid.v4()}`;
        return (client as any).createReceiver(AmqpRequestClient._getManagementPath(entityPath), <any>{
            name: receiverLinkName,
            attach: {
                target: {
                    address: receiverLinkName
                }
            },
            // Clear out reattach - we'll handle it ourselves
            reattach: null
        }, session);
    }

    private static _getManagementPath(entityPath: string) {
        return path.posix.join(entityPath, '$management');
    }

    private _pendingRequests: Map<string, (response: AmqpResponse<any>) => void> = new Map();
    private _requestTimeouts: Map<string, NodeJS.Timer> = new Map();
    private _requestTerminations: Map<string, () => void> = new Map();

    constructor(private _path: string, private _session: Session, private _sender: SenderLink, private _receiver: ReceiverLink, private _clientDispose: () => void, private _logFn: LogFn) {
        super();

        this._listenToResponses();
        this._isListening = true;

        // We reinitialize our listener each time it reconnects
        this._receiver.on('attached', () => {
            if (!this._isListening) {
                this._logFn(namespace, `Attached response receiver on entity ${this._path}`);
                this.emit(AmqpRequestClient.LINK_ATTACHED, this._receiver.name, this._path, false);
                this._listenToResponses();
                this._isListening = true;
            }
        });

        // We clean up any pending requests each time the listener disconnects
        this._receiver.on('detached', (info: any) => {
            this._logFn(namespace, `Detached response receiver on entity ${this._path}`);
            this.emit(AmqpRequestClient.LINK_DETACHED, this._receiver.name, this._path, false, info);
            this._isListening = false;
            this._terminateRequests();
        });

        // We don't really need to do anything when the listener attaches or detaches.
        // We'll just check it each time we try to send. Log here in case people
        // want to debug
        this._sender.on('attached', () => {
            this._logFn(namespace, `Attached request sender on entity ${this._path}`);
            this.emit(AmqpRequestClient.LINK_ATTACHED, this._sender.name, this._path, true);
        });

        this._sender.on('detached', (info: any) => {
            this._logFn(namespace, `Detached request sender on entity ${this._path}`);
            this.emit(AmqpRequestClient.LINK_DETACHED, this._sender.name, this._path, true, info);
        });
    }

    renewLock(lockToken: string): Promise<void> {
        // Why in the world are we reordering the bytes of the delivery tags here?
        // Good question! If you try to turn a Guid into a Buffer in .NET, the
        // bytes of the first three groups get flipped within the group, but the
        // last two groups don't get flipped, so we end up with the byte order
        // below. This is the order of bytes needed to make Service Bus recognize
        // the token.
        const lockTokenBytes = uuid.parse(lockToken);
        const reorderedLockToken = uuid.unparse(Buffer.from([
            lockTokenBytes[3],
            lockTokenBytes[2],
            lockTokenBytes[1],
            lockTokenBytes[0],

            lockTokenBytes[5],
            lockTokenBytes[4],

            lockTokenBytes[7],
            lockTokenBytes[6],

            lockTokenBytes[8],
            lockTokenBytes[9],

            lockTokenBytes[10],
            lockTokenBytes[11],
            lockTokenBytes[12],
            lockTokenBytes[13],
            lockTokenBytes[14],
            lockTokenBytes[15]
        ]));

        // TODO (868611): switch to translator format once it supports arrays
        // and uuids properly. See
        // https://github.com/noodlefrenzy/node-amqp10/issues/276 for more
        //
        // Ideally, we would want something like this instead:
        //
        // const body = translator(['map',
        //     ['string', 'lock-tokens'], ['array', 'uuid', reorderedLockToken]
        // ]);
        const body = new ForcedType('map', {
            'lock-tokens': new AMQPArray([reorderedLockToken], 0x98)
        });

        return this._sendRequest<any, any>({
            applicationProperties: {
                operation: 'com.microsoft:renew-lock'
            },
            body: new DescribedType(0x77, body)
        }).then(() => { });
    }

    private _sendRequest<T, S>(request: AmqpRequest<T>): Promise<AmqpResponse<S>> {
        if (!this._sender.canSend() || this._receiver.state() !== 'attached') {
            return Promise.reject(buildError(
                ErrorNames.Internal.RequestFailure,
                `Cannot initiate request because management sender and/or receiver are detached`,
                {
                    status: HttpStatus.SERVICE_UNAVAILABLE
                }
            ));
        }

        const messageId = uuid.v4();

        request.properties = {
            // Provide a message id (for correlation)
            messageId: messageId,
            // Provide the receiver link as the receiver of the response to this
            // request
            replyTo: this._receiver.name
        };

        request.applicationProperties['com.microsoft:server-timeout'] = constants.amqpRequestTimeout;

        // Put the handler on the messageId before sending to make sure we get
        // the response
        const result = new Promise<AmqpResponse<S>>((resolve, reject) => {

            const cleanupRequest = () => {
                clearTimeout(this._requestTimeouts.get(messageId));
                this._pendingRequests.delete(messageId);
                this._requestTimeouts.delete(messageId);
                this._requestTerminations.delete(messageId);
            };

            // We may never get a response back from the server. Since there are
            // different links for sending and receiving, we have to manage the
            // timeout ourselves.
            this._requestTimeouts.set(messageId, setTimeout(() => {
                this._logFn(namespace, `Request with correlationId ${messageId} did not complete within the configured timeout: ${constants.amqpRequestTimeout}`);
                cleanupRequest();
                reject(buildError(
                    ErrorNames.Internal.RequestTimeout,
                    `Didn't receive a response within the allotted timeout: ${constants.amqpRequestTimeout}`,
                    {
                        status: HttpStatus.GATEWAY_TIMEOUT,
                    }
                ));
            }, constants.amqpRequestTimeout));

            // This gives us a handle to reject the message if we have to
            // terminate the request for some reason
            this._requestTerminations.set(messageId, () => {
                cleanupRequest();
                reject(buildError(
                    ErrorNames.Internal.RequestTerminated,
                    `Request with correlationId ${request.properties.messageId} terminated due to disconnect`
                ));
            });

            // If all goes well, we'll end up here. This is where we do the
            // main processing of a response that we receive for a message
            this._pendingRequests.set(messageId, response => {
                try {
                    cleanupRequest();
                    if (response.applicationProperties.statusCode >= 200 && response.applicationProperties.statusCode < 300) {
                        resolve(response);
                    } else {
                        this._logFn(namespace, `Received an error when sending message with correlationId ${request.properties.messageId}`);
                        reject(buildError(
                            ErrorNames.Internal.RequestFailure,
                            `Request failed with error: ${response.applicationProperties.statusDescription}`,
                            {
                                status: response.applicationProperties.statusCode,
                                errorType: (<any>response.applicationProperties).errorCondition,
                                trackingId: (<any>response.applicationProperties)['com.microsoft:tracking-id']
                            }
                        ));
                    }
                } catch (e) {
                    reject(e);
                }
            });
        });

        // It is possible (and strangely common) for us to receive the response
        // back from Service Bus before we get back the acknowledgement for the
        // request we sent (i.e. the result of the send() Promise). Listening
        // to both promises in parallel allows us to be resilient to this by:
        // 
        //   1. Not waiting until the request completes when the response comes
        //      back first and
        //   2. Reacting to failures on the response immediately if it finishes
        //      before the request so we can fail fast and avoid the dreaded
        //      UnhandledPromiseRejection error
        //
        this._logFn(namespace, `Sending message on path ${this._path} with correlationId ${request.properties.messageId}: ${JSON.stringify(request)}`);
        return new Promise<AmqpResponse<S>>((resolve, reject) => {
            this._sender.send(request)
                .then(() => {
                    this._logFn(namespace, `Sent message with correlationId ${request.properties.messageId}`);
                })
                .catch(err => {
                    this._logFn(namespace, `Failed to send message with correlationId ${request.properties.messageId}`);
                    reject(buildError(ErrorNames.Internal.RequestFailure, `Failed to send request with error: ${err && err.message ? err.message : JSON.stringify(err)}`));
                });

            result
                .then(resolve)
                .catch(reject);
        });
    }

    private _listenToResponses() {
        this._logFn(namespace, `Listening on request receiver ${this._receiver.name}`);
        this._receiver.on('message', (message: AmqpResponse<any>) => {
            this._logFn(namespace, `Received response on path ${this._path} with correlationId ${message.properties.correlationId} and status ${message.applicationProperties.statusCode}`);

            if (this._pendingRequests.has(message.properties.correlationId)) {
                // Get the handler and remove its entry in the map
                const handler = this._pendingRequests.get(message.properties.correlationId);
                this._pendingRequests.delete(message.properties.correlationId);

                // Handle the response
                handler(message);
            } else {
                this._logFn(namespace, `No handler available for correlationId ${message.properties.correlationId}`);
                this.emit(AmqpRequestClient.REQUEST_CLIENT_ERROR, buildError(ErrorNames.Internal.OrphanedResponse, `Received an orphaned response on request client ${this._sender.name}`));
            }
        });
    }

    private _terminateRequests(): void {
        for (const terminationFn of this._requestTerminations.values()) {
            terminationFn();
        }
    }

    dispose() {
        Array.from(this._requestTimeouts.values()).map(timeout => clearTimeout(timeout));
        this._terminateRequests();
        this.removeAllListeners();
        (this._session as any).end({ dispose: true });
        this._session.removeAllListeners();
        this._sender.removeAllListeners();
        this._receiver.removeAllListeners();
        this._clientDispose();
    }
}
