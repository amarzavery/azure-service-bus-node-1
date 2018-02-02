import { ClientManager } from './clientManager';
import { TopicImpl } from './topic';
import { QueueImpl } from './queue';
import constants from './constants';
import { Topic, Queue, LogFn } from './types';
import { debug } from './utility';

const namespace = `${constants.debugNamespace}:client`;

export class Client {

    /**
     * Creates a service bus messaging client using the provided service bus
     * connection string
     *
     * @param  {string} connectionString A service bus connection string
     * @return {Client}                  A service bus client
     */
    public static createFromConnectionString(connectionString: string, logFn?: LogFn): Client {
        return new Client(connectionString, logFn);
    }

    private _clientManager: ClientManager;
    private _topics: Map<string, Topic> = new Map();
    private _queues: Map<string, Queue> = new Map();

    private constructor(connectionString: string, private _logFn: LogFn) {
        if (!this._logFn) {
            this._logFn = debug;
        }
        this._clientManager = new ClientManager(connectionString, this._logFn);
    }

    /**
     * Gets a handle for sending and receiving messages on a service bus topic
     * within the current service bus namespace. NOTE: this does not create the
     * topic or ensure it exists. Please see the service bus management APIs
     * (azure-sb package on npm) to create/verify a topic's existence.
     *
     * @param  {string} topicName The name of the topic
     * @return {Topic}            A handle for sending and receiving on the topic
     */
    getTopic(topicName: string): Topic {
        if (!this._topics.has(topicName)) {
            this._topics.set(topicName, new TopicImpl(topicName, this._clientManager, this._logFn));
            this._logFn(namespace, `Initialized topic ${topicName}`);
        }

        return this._topics.get(topicName);
    }

    /**
     * Gets a handle for sending and receiving messages on a service bus queue
     * within the current service bus namespace. NOTE: this does not create the
     * queue or ensure it exists. Please see the service bus management APIs
     * (azure-sb package on npm) to create/verify a queue's existence.
     *
     * @param  {string} queueName The name of the queue
     * @return {Queue}            A handle for sending and receiving on the queue
     */
    getQueue(queueName: string): Queue {
        if (!this._queues.has(queueName)) {
            this._queues.set(queueName, new QueueImpl(queueName, this._clientManager, this._logFn));
            this._logFn(namespace, `Initialized queue ${queueName}`);
        }

        return this._queues.get(queueName);
    }

    /**
     * Disposes of the connection to the service bus, freeing up resources
     * dedicated to the client, but also invalidating any further use of the
     * client.
     *
     * @return {Promise<void>}
     */
    async dispose(): Promise<void> {
        await this._clientManager.dispose();
        this._logFn = null;
    }
}
