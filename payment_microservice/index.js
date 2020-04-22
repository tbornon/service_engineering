const mysql = require('mysql');
const express = require('express');
const { EventStoreDB, ReadStorageDB } = require('./Database');
const RabbitMQ = require('./RabbitMQ');
const Payment = require('./Payment')
const app = express();
const EXCHANGES = require('./Exchanges');
const STATUS = require('./Status');
const PORT = 3000;

console.log("PAYMENT_MICROSERVICE")

var EventStoreConnection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});
var EventStore = new EventStoreDB(EventStoreConnection, "EventStoreDB_payment", "EventStore");

var ReadStorageConnection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});
var ReadStorage = new ReadStorageDB(ReadStorageConnection, "ReadStorageDB_payment", "ReadStorage");

var rabbitMQ = new RabbitMQ('amqp://rabbitmq');

function initialisation(callback) {
    return new Promise((resolve, reject) => {
        let eventStoreInit = EventStore.initialize();
        let readStorageInit = ReadStorage.initialize();
        let rabbitMQInit = rabbitMQ.initialize();
        configureWebServer();

        Promise.all([eventStoreInit, readStorageInit, rabbitMQInit])
            .then(() => {
                Payment.setQueueConnection(rabbitMQ.connection);
                Payment.setSqlConnection(EventStore.connection);
                return subscribePayment(rabbitMQ.connection);
            })
            .then(resolve)
            .catch(reject);
    });
}

function configureWebServer() {
    app.use(express.urlencoded({ extended: true }));
    app.use(express.json());

    app.post("/payment/create", (req, res, next) => {
        Payment.create(req.body.amount, req.body.clientId, (err, id) => {
            if (err) {
                res.status(500).json(err);
            }
            else {
                res.setHeader("Location", "http://localhost:3000/payment/" + id)
                res.send();
            }
        })
    });
    app.post("/payment/accept/:id", (req, res, next) => {
        Payment.accept(req.params.id, () => {
            res.json({})
        });
    });
    app.post("/payment/refuse/:id", (req, res, next) => {
        Payment.refuse(req.params.id, () => {
            res.json({})
        });
    });

    app.get("/payment/", (req, res, next) => {
        ReadStorage.getAllPayments(tickets => {
            res.json(tickets)
        })
    });

    app.get('/payment/:id', (req, res, next) => {
        ReadStorage.getPayment(req.params.id, ticket => {
            res.send(ticket || {});
        });
    });

    app.listen(PORT);
}

function subscribePayment(connection) {
    return new Promise((resolve, reject) => {
        // payment_generate queue
        connection.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }
            var queue = 'payment_generate';

            channel.assertQueue(queue, {
                durable: false
            });
            channel.prefetch(1);
            console.log(' [x] Awaiting payment_generate');

            channel.consume(queue, function reply(msg) {
                var dataReceived = JSON.parse(msg.content);
                console.log("Received message for payment generation: ", dataReceived);

                Payment.create(dataReceived.amount, dataReceived.clientId, (err, paymentId) => {
                    console.log("Payment created %s", paymentId)
                    if (err) channel.nack(msg);
                    else {
                        let responseData = {
                            paymentId,
                            clientId: dataReceived.clientId
                        };

                        channel.sendToQueue(msg.properties.replyTo,
                            Buffer.from(JSON.stringify(responseData)), {
                            correlationId: msg.properties.correlationId
                        });

                        channel.ack(msg);
                    }
                });
            });
        });

        // payment_refund queue
        connection.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }
            var queue = 'payment_refund';

            channel.assertQueue(queue, {
                durable: false
            });
            channel.prefetch(1);
            console.log(' [x] Awaiting payment_refund');
            channel.consume(queue, function reply(msg) {
                var dataReceived = JSON.parse(msg.content);
                console.log("Received message for refund: ", dataReceived);

                Payment.create(dataReceived.amount, dataReceived.clientId, (err, paymentId) => {
                    Payment.refund(paymentId, dataReceived.clientId, () => {
                        console.log("Refund created %s", paymentId)
                        if (err) channel.nack(msg);
                        else {
                            let responseData = {
                                paymentId
                            };

                            channel.sendToQueue(msg.properties.replyTo,
                                Buffer.from(JSON.stringify(responseData)), {
                                correlationId: msg.properties.correlationId
                            });

                            channel.ack(msg);
                        }
                    })
                });
            });
        });


        // Internal channel
        connection.createChannel(function (err, channel) {
            if (err)
                return reject(err);

            Object.keys(EXCHANGES).forEach(exchangeName => {
                channel.assertExchange(exchangeName, 'direct', {
                    durable: false
                });
            });

            channel.assertQueue('',
                {
                    exclusive: true
                },
                function (err, q) {
                    if (err)
                        return reject(err);

                    Object.keys(EXCHANGES).forEach(exchange => {
                        Object.keys(EXCHANGES[exchange]).forEach(key => {
                            channel.bindQueue(q.queue, exchange, EXCHANGES[exchange][key]);
                            console.log("Queue binded to exchange %s with routing key : %s", exchange, EXCHANGES[exchange][key])
                        })
                    })


                    channel.consume(q.queue, function (msg) {
                        console.log(" [x] %s: '%s'", msg.fields.routingKey, msg.content.toString());
                        let dataReceived = JSON.parse(msg.content);
                        EventStore.addToEventStore({
                            internalId: dataReceived.internalId,
                            other: dataReceived.other,
                            operation: msg.fields.routingKey
                        });

                        switch (dataReceived.operation) {
                            case EXCHANGES.Payment.PaymentGenerated:
                                ReadStorage.createPayment(dataReceived.internalId);
                                break;
                            case EXCHANGES.Payment.PaymentSetAmount:
                                ReadStorage.setPaymentAmount(dataReceived.internalId, dataReceived.other);
                                break;
                            case EXCHANGES.Payment.PaymentAccepted:
                                ReadStorage.setPaymentStatus(dataReceived.internalId, STATUS.Accepted);
                                break;
                            case EXCHANGES.Payment.PaymentRefused:
                                ReadStorage.setPaymentStatus(dataReceived.internalId, STATUS.Refused);
                                break;
                            case EXCHANGES.Payment.PaymentRefunded:
                                ReadStorage.setPaymentStatus(dataReceived.internalId, STATUS.Refunded);
                                break;
                            case EXCHANGES.Payment.PaymentSetClientId:
                                ReadStorage.setPaymentClientId(dataReceived.internalId, dataReceived.other);
                                break;
                        }
                    }, {
                        noAck: true
                    });

                    resolve();
                });
        });
    });
}


setTimeout(() => {
    initialisation()
        .then(() => console.log("Initialization finished with success"))
        .catch(error => console.error("An error happened while configuring :", error));
}, 10000)