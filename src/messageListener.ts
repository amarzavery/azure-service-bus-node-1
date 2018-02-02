import { EventEmitter } from 'events';
import { ReceiverLink } from '@azure-iot/amqp10';

import { MessageListener } from './types';
import { AmqpRequestClient } from './amqpRequestClient';
import { CreditManager } from './creditManager';

export class MessageListenerImpl extends EventEmitter implements MessageListener {

    private _isListening: boolean = true;
    get isListening(): boolean {
        return this._isListening;
    }

    private _cleanupCallback: () => void = () => { };
    private _creditManager: CreditManager;

    get pendingSettleCount(): number {
        return this._creditManager ? this._creditManager.pendingSettleCount : 0;
    }

    initialize(cleanupCallback: () => void, creditManager: CreditManager) {
        this._cleanupCallback = cleanupCallback;
        this._creditManager = creditManager;
        this._isListening = true;
    }

    dispose(): void {
        this.removeAllListeners();
        this.disposeInternals();
    }

    disposeInternals(): void {
        if (this.isListening) {
            this._cleanupCallback();
            this._isListening = false;
        }
    }
}
