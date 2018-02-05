# azure-service-bus-node

Promise-based AMQP 1.0 client for sending and receiving messages on
[Azure Service Bus](https://azure.microsoft.com/en-us/services/service-bus/)
topics and queues using Node.JS/ES6.

## Requirements

This package is written in TypeScript and compiled to ES6/ES2015. As a result,
you must be running [Node.JS](https://nodejs.org/) 6.x or higher for this
package to function correctly.

## Getting Started

To use this package, simply run the following from your favorite terminal
editor inside the folder for your project:

```sh
npm install --save azure-servicebus
```

At its most simple, this package allows you to connect to an Azure Service Bus
and then send or receive messages from a queue or topic on that Service Bus.
There are additional configuration options to provide more capabilities if
needed.

### Send a Message

Sending messages on a queue or topic is identical. Once you have gotten the
queue or topic, simply call `send()` and pass in a `BrokeredMessage`. The call
will return a Promise which resolves if the send succeeds or rejects if the send
fails.

#### Using a Queue

```ts
import {Client as ServiceBusClient, BrokeredMessage} from 'azure-servicebus';

// Replace this with your actual connection string
const connectionString = '<your-service-bus-connection-string>';

// Initialize the client. You should only do this once per Service Bus
const client = ServiceBusClient.createFromConnectionString(connectionString);

// Get the topic
const queue = client.getQueue('my-queue');

// Create and send the message
const message = new BrokeredMessage({ hello: 'again' });
queue.send(message)
    .then(() => { console.log('done!'); })
    .catch(err => { console.log('something went wrong', err); });
```

#### Using a Topic

```ts
import {Client as ServiceBusClient, BrokeredMessage} from 'azure-servicebus';

// Replace this with your actual connection string
const connectionString = '<your-service-bus-connection-string>';

// Initialize the client. You should only do this once per Service Bus
const client = ServiceBusClient.createFromConnectionString(connectionString);

// Get the topic
const topic = client.getTopic('my-topic');

// Create and send the message
const message = new BrokeredMessage({ hello: 'world' });
topic.send(message)
    .then(() => { console.log('done!'); })
    .catch(err => { console.log('something went wrong', err); });
```

### Listen for Messages

You can listen for messages on a queue or topic/subscription using the
`onMessage()` method on a topic or a queue. The APIs are identical between queues
and topics except topics have an extra parameter at the beginning specifying
the name of the subscription to listen to. The call returns a subscription on
which you can call `unsubscribe()` to stop receiving messages. You can also call
`onDeadLetteredMessage()` with the same parameters to listen to the dead letter
queue of a queue or topic/subscription.

Each time you call `onMessage()` on a topic or queue, a new listener is set up.
You can set up multiple listeners on the same messaging entity by calling
`onMessage()` multiple times, but doing so for the same queue or
topic/subscription combination is usually undesirable.

#### Using a Queue

```ts
import {Client as ServiceBusClient} from 'azure-servicebus';

// Replace this with your actual connection string
const connectionString = '<your-service-bus-connection-string>';

// Initialize the client. You should only do this once per Service Bus
const client = ServiceBusClient.createFromConnectionString(connectionString);

// Get the topic
const queue = client.getQueue('my-queue');

// Create and send the message
const listener = queue.onMessage(message => {
    console.log('received message', message);
});

// When you don't want to listen to messages any more, simply dispose
// listener.dispose();
```

#### Using a Topic/Subscription

```ts
import {Client as ServiceBusClient} from 'azure-servicebus';

// Replace this with your actual connection string
const connectionString = '<your-service-bus-connection-string>';

// Initialize the client. You should only do this once per Service Bus
const client = ServiceBusClient.createFromConnectionString(connectionString);

// Get the topic
const topic = client.getTopic('my-topic');

// Create and send the message
const listener = topic.onMessage('my-subscription', message => {
    console.log('received message', message);
});

// When you don't want to listen to messages any more, simply dispose
// listener.dispose();
```

### Message Acknowledgement

When a message is received in `ReceiveMode.PeekLock`, you are expected to
respond to the message with a certain acknowledgement. Each acknowledgement is
exposed as a method on the `BrokeredMessage` passed to the handler (e.g.
`message.complete()`). The options are:

 * **`complete`** - Instructs the Service Bus to remove the message. This is the
   most common acknowledgement and indicates that the message was processed
   successfully
 * **`abandon`** - Returns the message back to the Service Bus so that it can be
   redelivered (if applicable). This is useful if a certain client is unable to
   handle a message, but others may be able to.
 * **`deadLetter`** - Pushes the message to the dead letter queue for the queue or
   topic/subscription. The message will not be redelivered to an `onMessage()`
   handler, but can be received using the dead letter queue. This usually
   indicates an issue with the processing of a message that is unrecoverable.

### Configuring a Message Receiver

There are multiple ways that you can receive messages on a Service Bus queue or
topic/subscription. The following configuration options are available:

 * **`receiveMode`** (*ReceiveMode*) - Enumerated value specifying how messages
   should be received. There are two possible values, `PeekLock` and
   `ReceiveAndDelete`. Using `PeekLock` gives you a lock on the message when it
   is delivered. Once you have processed the message, you must call one of the
   acknowledgement methods on the `BrokeredMessage` to settle the message with
   Service Bus. Choosing `ReceiveAndDelete` causes the message to be
   automatically removed from the Service Bus upon delivery. You do not need to
   call any of the acknowledgement methods in this mode. (default:
   `ReceiveMode.PeekLock`)

 * **`autoComplete`** (*boolean*) - Indicates whether or not the message should be
   automatically completed (equivalent to calling `complete()` on the message)
   when the message handler completes without throwing an error. This behavior
   works whether the method is synchronous or returns a Promise. Setting to true
   does not automatically call `abandon()` or `deadLetter()` on failure.
   (default: `true`)

 * **`autoRenewTimeout`** (*number*) - The number of milliseconds to continue to
   automatically renew a receiver's lock on a message. After this time period,
   the lock will not renew and the message will eventually time out and be
   redelivered (if applicable). If you would like the lock to be renewed
   indefinitely, set this to `Infinity`. If you do not want the lock to be
   renewed at all, set this to `0`. (default: 5 minutes)

 * **`maxConcurrentCalls`** (*number*) - The maximum number of messages that can be
   processed concurrently. Each time a message is received on that handler, it
   counts against this max. Once you have acknowledged a message (or immediately
   upon delivery when using `ReceiveMode.ReceiveAndDelete`), it will be
   subtracted from the number of concurrent messages. (default: `1`)

You can customize these options by passing an optional object as the last
parameter to the `onMessage()` or `onDeadLetteredMessage()` method for both
queues and topics:

```ts
import {Client as ServiceBusClient, ReceiveMode} from 'azure-servicebus';

// Replace this with your actual connection string
const connectionString = '<your-service-bus-connection-string>';

// Initialize the client. You should only do this once per Service Bus
const client = ServiceBusClient.createFromConnectionString(connectionString);

// Get the topic
const queue = client.getQueue('my-queue');

// Create and send the message
const listener = queue.onMessage(message => {
    console.log('received message', message);
    message.complete();
}, {
    receiveMode: ReceiveMode.PeekLock,
    autoComplete: false,
    autoRenewTimeout: 1000 * 60 * 60, // 1 hour
    maxConcurrentCalls: 1000
});

// When you don't want to listen to messages any more, simply dispose
// listener.dispose();
```
