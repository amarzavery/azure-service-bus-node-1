import * as createDebugNamespace from 'debug';

export interface ConnectionString {
    Endpoint: string;
    SharedAccessKeyName: string;
    SharedAccessKey: string;
}

export function parseConnectionString(connectionString: string): ConnectionString {
    return <ConnectionString>connectionString.split(';')
        .map(part => {
            const splitIndex = part.indexOf('=');
            return [part.substring(0, splitIndex), part.substring(splitIndex + 1)];
        })
        .reduce((acc, [key, val]) => Object.assign(acc, { [key]: val }), {});
}

export function setIfDefined(obj: any, key: string, value: any) {
    if (value !== undefined) {
        obj[key] = value;
    }
}

export function verifyType(value: any, type: 'string' | 'number') {
    if (value != null && typeof value !== type) {
        throw new TypeError(`Invalid type provided. Value must be a ${type}`);
    }
}

export function verifyClass(value: any, clazz: Function, className: string) {
    if (value != null && !(value instanceof clazz)) {
        throw new TypeError(`Invalid type provided. Value must be an instance of ${className}`);
    }
}

export function debug(namespace: string, message: string): void {
    createDebugNamespace(namespace)(message);
}