export const ErrorNames = {
    Link: {
        Detach: 'ServiceBus:Link:DetachError',
        NotFound: 'ServiceBus:Link:NotFoundError',
        CreditManagerMissing: 'ServiceBus:Link:CreditManagerMissingError',
    },
    Message: {
        LockRenewalTimeout: 'ServiceBus:Message:LockRenewalTimeoutError',
        LockRenewalFailure: 'ServiceBus:Message:LockRenewalError',
        SettleFailure: 'ServiceBus:Message:SettleError',
    },
    Internal: {
        Unknown: 'ServiceBus:Internal:UnknownError',
        RequestTimeout: 'ServiceBus:Internal:RequestTimeout',
        RequestFailure: 'ServiceBus:Internal:RequestFailure',
        RequestTerminated: 'ServiceBus:Internal:RequestTerminated',
        OrphanedResponse: 'ServiceBus:Internal:OrphanedResponse',
    },
    Send: {
        Timeout: 'ServiceBus:Send:Timeout',
        Rejected: 'ServiceBus:Send:Rejected',
        Disposed: 'ServiceBus:Send:Disposed'
    },
    Amqp: {
        InternalError: 'ServiceBus:Amqp:InternalError',
        NotFound: 'ServiceBus:Amqp:NotFound',
        UnauthorizedAccess: 'ServiceBus:Amqp:UnauthorizedAccess',
        DecodeError: 'ServiceBus:Amqp:DecodeError',
        ResourceLimitExceeded: 'ServiceBus:Amqp:ResourceLimitExceeded',
        NotAllowed: 'ServiceBus:Amqp:NotAllowed',
        InvalidField: 'ServiceBus:Amqp:InvalidField',
        NotImplemented: 'ServiceBus:Amqp:NotImplemented',
        ResourceLocked: 'ServiceBus:Amqp:ResourceLocked',
        PreconditionFailed: 'ServiceBus:Amqp:PreconditionFailed',
        ResourceDeleted: 'ServiceBus:Amqp:ResourceDeleted',
        IllegalState: 'ServiceBus:Amqp:IllegalState',
        FrameSizeTooSmall: 'ServiceBus:Amqp:FrameSizeTooSmall',
        Unknown: 'ServiceBus:Amqp:Unknown'
    }
};

export class ServiceBusError extends Error {
    context?: any;
}

export function throwError(name: string, message: string, context?: any): never {
    throw buildError(name, message, context);
}

export function buildError(name: string, message: string, context?: any): Error {
    const error = new ServiceBusError(message);
    error.name = name;
    error.context = context;
    return error;
}

export function wrapError(error: any): Error {
    if (error && typeof error.name === 'string' && error.name.startsWith('ServiceBus:')) {
        // It's a known error type
        return error as Error;
    } else if (error && Array.isArray(error.value) && error.value[0] && error.value[0].typeName === 'symbol' && error.value[0].value) {
        // It's most likely an amqp error (list sourced from www.amqp.org/sites/amqp.org/files/amqp.pdf section 2.8.15)
        const amqpSymbol = error.value[0].value;
        const message = error.value[1] && error.value[1].value;
        switch (amqpSymbol) {
            case 'amqp:internal-error':
                return buildError(ErrorNames.Amqp.InternalError, `An internal error occurred on the amqp server`, { message });
            case 'amqp:not-found':
                return buildError(ErrorNames.Amqp.NotFound, `The messaging entity was not found`, { message });
            case 'amqp:unauthorized-access':
                return buildError(ErrorNames.Amqp.UnauthorizedAccess, `The client does not have access to the messaging entity`, { message });
            case 'amqp:decode-error':
                return buildError(ErrorNames.Amqp.DecodeError, `Data could not be decoded`, { message });
            case 'amqp:resource-limit-exceeded':
                return buildError(ErrorNames.Amqp.ResourceLimitExceeded, `The messaging entity has exceeded the maximum storage capacity`, { message });
            case 'amqp:not-allowed':
                return buildError(ErrorNames.Amqp.NotAllowed, `An AMQP frame was constructed or used incorrectly`, { message });
            case 'amqp:invalid-field':
                return buildError(ErrorNames.Amqp.InvalidField, `An invalid field was passed in the body of an AMQP frame`, { message });
            case 'amqp:not-implemented':
                return buildError(ErrorNames.Amqp.NotImplemented, `The requested functionality has not been implemented`, { message });
            case 'amqp:resource-locked':
                return buildError(ErrorNames.Amqp.ResourceLocked, `The requested entity has been locked by another client`, { message });
            case 'amqp:precondition-failed':
                return buildError(ErrorNames.Amqp.PreconditionFailed, `A precondition for the request failed`, { message });
            case 'amqp:resource-deleted':
                return buildError(ErrorNames.Amqp.ResourceDeleted, `The messaging entity has been deleted`, { message });
            case 'amqp:illegal-state':
                return buildError(ErrorNames.Amqp.IllegalState, `Invalid AMQP frame sent given the current Session state`, { message });
            case 'amqp:frame-size-too-small':
                return buildError(ErrorNames.Amqp.FrameSizeTooSmall, `The maximum frame size is too small for the requested data payload`, { message });
            default:
                return buildError(ErrorNames.Amqp.Unknown, `Encountered an unknown AMQP error: ${amqpSymbol}`, { message });
        }
    } else {
        return buildError(ErrorNames.Internal.Unknown, `Encountered unknown error: ${error && error.message ? error.message : JSON.stringify(error)}`);
    }
}
