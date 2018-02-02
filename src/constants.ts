export default {
    debugNamespace: 'servicebus',
    amqpRequestTimeout: 15000,
    serviceBusDeliveryTimeout: 30000,
    serviceBusServerTimeout: 60000,
    renewThreshold: 0.75,
    reattachInterval: 5000,
    autoRenewTimeout: 1000 * 60 * 5, // 5 minutes
    maxConcurrentCalls: 1,
    handleMax: 255,
    amqpClientCleanupDelayMs: 600000, // 10 minutes
    defaultSendTimeout: 15000
};
