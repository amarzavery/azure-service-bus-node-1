import * as url from 'url';
import * as uuid from 'node-uuid';
import { Client as AMQPClient, Policy, Constants as AmqpConstants, SenderLink, ReceiverLink } from '@azure-iot/amqp10';
import { LogFn } from './types';

import { parseConnectionString } from './utility';
import constants from './constants';

const namespace = `${constants.debugNamespace}:clientManager`;

interface AMQPClientState {
    id: string;
    amqpClient: Promise<AMQPClient>;
    linksCount: number;
    cleanupTimer: NodeJS.Timer;
}

export interface CreateSenderOptions {
    address: string;
    policyOverrides?: Policy.PolicyBase.SenderLink;
}

export interface CreateReceiverOptions {
    address: string;
    policyOverrides?: Policy.PolicyBase.ReceiverLink;
}

export interface CreateLinkGroupOptions {
    senders: CreateSenderOptions[];
    receivers: CreateReceiverOptions[];
}

export interface LinkGroup {
    senders: Promise<SenderLink>[];
    receivers: Promise<ReceiverLink>[];
}

export class ClientManager {
    private _endpoint: string;
    private _sharedAccessKeyName: string;
    private _sharedAccessKey: string;

    private _clients: AMQPClientState[] = [];

    constructor(connectionString: string, private _logFn: LogFn) {
        const { Endpoint, SharedAccessKeyName, SharedAccessKey } =
            parseConnectionString(connectionString);
        this._endpoint = Endpoint;
        this._sharedAccessKeyName = SharedAccessKeyName;
        this._sharedAccessKey = SharedAccessKey;
    }

    /**
     * Gets a AMQP client.
     * 
     * @param {number} numLinks number of links required on the client
     * @returns {{ dispose: () => void, client: Promise<AMQPClient> }}
     */
    getAMQPClient(numLinks: number = 1): { dispose: () => void, client: Promise<AMQPClient> } {
        const clientState = this._getAMQPClient(numLinks);
        return {
            dispose: () => {
                if ((clientState.linksCount - numLinks) >= 0) { clientState.linksCount -= numLinks; }
                else { clientState.linksCount = 0; }
                if (clientState.linksCount === 0) {
                    this._scheduleCleanup(clientState);
                }
                this._logFn(namespace, `Disposed ${numLinks} links on client ${clientState.id}. Total links = ${clientState.linksCount}`);
            },
            client: clientState.amqpClient
        };
    }


    /**
     * Disposes all AMQP clients
     */
    dispose(): Promise<void> {
        this._logFn(namespace, `Disposing ${this._clients.length} clients.`);
        return Promise.all(this._clients.map(async (clientState) => { await (await clientState.amqpClient).disconnect(); }))
            .then(() => {
                this._logFn(namespace, `Disposed ${this._clients.length} clients.`);
                this._clients = [];
            });
    }

    private _getAMQPClient(numLinks: number = 1): AMQPClientState {
        for (const clientState of this._clients) {
            if (clientState && ((clientState.linksCount + numLinks) <= constants.handleMax) && clientState.amqpClient) {
                clientState.linksCount += numLinks;
                clientState.cleanupTimer = null;
                this._logFn(namespace, `Got client ${clientState.id} with ${numLinks} links. Total links = ${clientState.linksCount}`);
                return clientState;
            }
        }

        const clientState: AMQPClientState = {
            id: `client$${uuid.v4()}`,
            amqpClient: this._createAMQPClient(),
            linksCount: numLinks,
            cleanupTimer: null
        };

        this._clients.push(clientState);
        return clientState;
    }

    private async _createAMQPClient(): Promise<AMQPClient> {
        const serviceBusHostName = url.parse(this._endpoint).host;
        this._logFn(namespace, `Creating AMQP connection for Service Bus ${serviceBusHostName}`);
        const client = new AMQPClient(Policy.merge(
            {
                senderLink: {
                    attach: <any>{
                        sndSettleMode: AmqpConstants.senderSettleMode.mixed
                    }
                }
            },
            Policy.ServiceBusQueue
        )); // Policy is the same for queue and topic

        // TODO: Use CBS for auth
        await client.connect(`amqps://${encodeURIComponent(this._sharedAccessKeyName)}:${encodeURIComponent(this._sharedAccessKey)}@${serviceBusHostName}`);
        this._logFn(namespace, `Finished creating AMQP connection for Service Bus ${serviceBusHostName}`);
        return client;
    }

    private async _cleanupClient(clientState: AMQPClientState): Promise<void> {
        const index = this._clients.indexOf(clientState);
        if (index > -1 && clientState.linksCount === 0) {
            this._clients.splice(index, 1); // Deleting from array to ensure this client doesn't get picked up to create links

            const amqpClient = await clientState.amqpClient;
            await amqpClient.disconnect();
            clientState.cleanupTimer = null;
            this._logFn(namespace, `Client ${clientState.id} disposed.`);
        }
    }

    private _scheduleCleanup(clientState: AMQPClientState): void {
        if (!clientState.cleanupTimer) {
            this._logFn(namespace, `Scheduling cleanup for client ${clientState.id}.`);
            clientState.cleanupTimer = setTimeout(async () => { await this._cleanupClient(clientState); }, constants.amqpClientCleanupDelayMs);
        }
    }
}