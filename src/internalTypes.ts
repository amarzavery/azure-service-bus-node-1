export interface AmqpMessage<T> {
    body: T;
    header?: {
        durable?: boolean;
        priority?: number;
        ttl?: number;
        firstAcquirer?: boolean;
        deliveryCount?: number;
    };
    messageAnnotations?: {
        'x-opt-enqueued-time'?: Date;
        'x-opt-sequence-number'?: number;
        'x-opt-partition-id'?: number;
        'x-opt-partition-key'?: string;
        'x-opt-locked-until'?: Date;
    };
    properties: {
        messageId: string;
        userId?: string;
        to?: string;
        subject?: string;
        replyTo?: string;
        correlationId?: string;
        contentType?: string;
        contentEncoding?: string;
        absoluteExpiryTime?: any;
        creationTime?: any;
        groupId?: string;
        groupSequence?: string;
        replyToGroupId?: string;
    };
    applicationProperties: { [key: string]: any };
}

export interface AmqpRequest<T> {
    body?: T;
    properties?: {
        messageId?: string;
        replyTo?: string;
    };
    applicationProperties: {
        operation: string;
        'com.microsoft:server-timeout'?: number;
    };
}

export interface AmqpResponse<T> {
    body?: T;
    properties?: {
        correlationId?: string;
    };
    applicationProperties: {
        statusCode: number;
        statusDescription: string;
    };
}
