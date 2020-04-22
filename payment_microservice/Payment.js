const uuid = require('uuid').v4;
const EXCHANGE_NAME = "Payment"
const EXCHANGES = require('./Exchanges')

function Payment() {
    this.queueConnection = null;
    this.channelConnection = null;
    this.sqlConnection = null;
}

Payment.prototype.setQueueConnection = connection => {
    this.queueConnection = connection;
    let self = this;

    this.queueConnection.createChannel(function (err, channel) {
        if (err)
            throw err;

        channel.assertExchange(EXCHANGE_NAME, 'direct', {
            durable: false
        });

        self.channelConnection = channel;
    });

    console.log("Payment.js connected to queue")
}

Payment.prototype.setSqlConnection = connection => {
    this.sqlConnection = connection;
}

Payment.prototype.create = (amount, clientId, callback = () => { }) => {
    if (amount != undefined && clientId != undefined) {
        let PaymentUuid = uuid();
        // Publish PaymentCreated
        publish(this.channelConnection, PaymentUuid, null, EXCHANGES.Payment.PaymentGenerated);
        // Publish PaymentSetAmount
        publish(this.channelConnection, PaymentUuid, amount, EXCHANGES.Payment.PaymentSetAmount);
        // Publish PaymentSetClient
        publish(this.channelConnection, PaymentUuid, clientId, EXCHANGES.Payment.PaymentSetClientId);

        callback(null, PaymentUuid);
    } else
        callback(new Error("MissingParameter"));
}


Payment.prototype._findClientId = paymentId => {
    return new Promise((resolve, reject) => {
        this.sqlConnection.query(`select other as clientId from EventStore where internalId = '${paymentId}' and operation = '${EXCHANGES.Payment.PaymentGenerated}'`, (err, result) => {
            if (err) return reject(err);
            if (result.length > 0) {
                resolve(result[0].clientId);
            }
            else
                reject(new Error('Payment not found'));
        })
    });
}

Payment.prototype.accept = (paymentId, callback = () => { }) => {
    findClientId(this.sqlConnection, paymentId)
        .then(clientId => {
            console.log(clientId)
            publish(this.channelConnection, paymentId, clientId, EXCHANGES.Payment.PaymentAccepted);
            callback();
        })
        .catch(callback);
}

Payment.prototype.refuse = (paymentId, callback = () => { }) => {
    findClientId(this.sqlConnection, paymentId)
        .then(clientId => {
            publish(this.channelConnection, paymentId, clientId, EXCHANGES.Payment.PaymentRefused);
            callback();
        })
        .catch(callback);
}

Payment.prototype.refund = (paymentId, clientId, callback = () => { }) => {
    publish(this.channelConnection, paymentId, clientId, EXCHANGES.Payment.PaymentRefunded);
    callback();
}

let publish = (channel, internalId, other, operation) => {
    channel.publish(
        EXCHANGE_NAME,
        operation,
        Buffer.from(JSON.stringify({
            internalId: internalId,
            other: other,
            operation: operation
        }))
    );
}

let findClientId = (sqlConnection, paymentId) => {
    return new Promise((resolve, reject) => {
        sqlConnection.query(`select other as clientId from EventStore where internalId = '${paymentId}' and operation = '${EXCHANGES.Payment.PaymentSetClientId}'`, (err, result) => {
            if (err) return reject(err);
            if (result.length > 0) {
                resolve(result[0].clientId);
            }
            else
                reject(new Error('Payment not found'));
        })
    });
}

module.exports = new Payment()