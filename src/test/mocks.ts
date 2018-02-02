import { EventEmitter } from 'events';
import { Client as AMQPClient, Policy, SenderLink, ReceiverLink } from '@azure-iot/amqp10';
import { CreateLinkGroupOptions, LinkGroup } from '../clientManager';
import * as uuid from 'node-uuid';

function getMockSender() {
    const sender: any = new EventEmitter();
    sender.send = (message: any, options?: any) => Promise.resolve({
        toDescribedType: (): any => ({ descriptor: 0x24 })
    });
    sender.canSend = () => true;
    sender.state = () => 'attached';
    sender.detach = () => {
        sender.removeAllListeners();
    };
    sender.forceDetach = () => {
        sender.removeAllListeners();
    };
    sender.name = uuid.v4();
    return sender;
}

function getMockReceiver() {
    const receiver: any = new EventEmitter();
    receiver.linkCredit = Infinity;

    receiver.sendMessage = (message: any) => {
        receiver.emit('message', message, {
            deliveryTag: uuid.parse(uuid.v4())
        });
    };

    receiver.sendDetach = (error: any) => receiver.emit('detached', error);
    receiver.sendAttach = () => receiver.emit('attached');

    receiver.state = () => 'attached';

    receiver.accept = () => { };
    receiver.modify = () => { };
    receiver.reject = () => { };
    receiver.release = () => { };
    receiver.detach = () => {
        receiver.removeAllListeners();
    };
    receiver.forceDetach = () => {
        receiver.removeAllListeners();
    };
    receiver.addCredits = () => { };
    receiver.name = uuid.v4();

    return receiver;
}

function getMockSession() {
    const session: any = new EventEmitter();
    session.begin = () => { };
    session.end = () => { };

    return session;
}

export const mockSession: any = getMockSession();

export const mockReceiver: any = getMockReceiver();

export const mockSender: any = getMockSender();

export const mockAmqpClient: any = {
    createSender: (address: string, policyOverrides?: Policy.PolicyBase.SenderLink): Promise<SenderLink> => {
        return Promise.resolve(mockSender);
    },
    createReceiver: (address: string, policyOverrides?: Policy.PolicyBase.ReceiverLink): Promise<ReceiverLink> => {
        return Promise.resolve(mockReceiver);
    },
    createSession: () => Promise.resolve(mockSession)
};

export const mockAmqpClientManager: any = {
    getAMQPClient: (numLinks: number): { dispose: () => void, client: Promise<AMQPClient> } => {
        return {
            dispose: () => { },
            client: Promise.resolve(mockAmqpClient)
        };
    }
};