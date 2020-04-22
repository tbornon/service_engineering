const mysql = require('mysql');
const express = require('express');
const { EventStoreDB, ReadStorageDB } = require('./Database');
const RabbitMQ = require('./RabbitMQ');
const Ticket = require('./Ticket')
const app = express();
const EXCHANGES = require('./Exchanges');
const PORT = 3000;

console.log("TICKET_MICROSERVICE")

var EventStoreConnection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});
var EventStore = new EventStoreDB(EventStoreConnection, "EventStoreDB_ticket", "EventStore");

var ReadStorageConnection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});
var ReadStorage = new ReadStorageDB(ReadStorageConnection, "ReadStorageDB_ticket", "ReadStorage");

var rabbitMQ = new RabbitMQ('amqp://rabbitmq');

/**
 * Initialize all the components of the service
 *   - EventStoreDB
 *   - ReadStorageDB
 *   - RabbitMQ connection
 *   - Express (web server) routes
 *   - Ticket command service
 */
function initialisation() {
    return new Promise((resolve, reject) => {
        let eventStoreInit = EventStore.initialize();
        let readStorageInit = ReadStorage.initialize();
        let rabbitMQInit = rabbitMQ.initialize();
        configureWebServer();

        Promise.all([eventStoreInit, readStorageInit, rabbitMQInit])
            .then(() => {
                Ticket.setQueueConnection(rabbitMQ.connection);
                Ticket.setSqlConnection(EventStore.connection)
                return subscribeTicket(rabbitMQ.connection);
            })
            .then(resolve)
            .catch(reject);
    });
}

/**
 * Configure all routes for the web server
 */
function configureWebServer() {
    app.use(express.urlencoded({ extended: true }));
    app.use(express.json());

    // Command service routes
    app.post("/ticket/create", (req, res, next) => {
        Ticket.create(req.body, (err, uuid) => {
            if (err) {
                console.error(err);
                res.status(500).send(err);
            }
            else {
                res.setHeader("Location", "http://localhost:3000/ticket/" + uuid);
                res.send();
            }
        });
    });

    app.post('/ticket/return', (req, res, next) => {
        Ticket.return(req.body, (uuid) => {
            res.setHeader("Location", "http://localhost:3000/payment/" + uuid);
            res.send();
        })
    });

    app.post('/ticket/order', (req, res, next) => {
        Ticket.order(req.body.clientId, (err, uuid) => {
            if (err) res.status(500).json(err);
            res.setHeader("Location", "http://localhost:3000/payment/" + uuid);
            res.send();
        })
    });

    // Used for debug purpose only
    app.post('/ticket/reserve', (req, res, next) => {
        Ticket.reserve(req.body.ticketId, req.body.clientId, (uuid) => {
            res.send({});
        })
    });

    // Used for debug purpose only
    app.post('/ticket/unreserve', (req, res, next) => {
        Ticket.unreserve(req.body.ticketId, req.body.clientId, (uuid) => {
            res.send({});
        })
    });

    // Query service routes
    app.get("/ticket/", (req, res, next) => {
        ReadStorage.getAllTickets(tickets => {
            res.json(tickets)
        })
    });

    app.get('/ticket/:id', (req, res, next) => {
        ReadStorage.getTicket(req.params.id, ticket => {
            res.send(ticket || {});
        });
    });

    app.listen(PORT);
}

/**
 * 
 */
function subscribeTicket() {
    return new Promise((resolve, reject) => {

        // ticket_reserved queue
        rabbitMQ.connection.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }
            var queue = 'ticket_reserved';

            channel.assertQueue(queue, {
                durable: false
            });
            channel.prefetch(1);
            console.log(' [x] Awaiting ticket_reserved');

            channel.consume(queue, function reply(msg) {
                var dataReceived = JSON.parse(msg.content);
                console.log("Received message for ticket reservation: ", dataReceived);

                Ticket.reserve(dataReceived.other, dataReceived.internalId, err => {
                    if (err) {
                        console.log("Réponse envoyée")
                        channel.sendToQueue(msg.properties.replyTo,
                            Buffer.from("error"), {
                            correlationId: msg.properties.correlationId
                        });

                        channel.ack(msg);
                    } else {
                        console.log("Réponse envoyée")
                        channel.sendToQueue(msg.properties.replyTo,
                            Buffer.from("available"), {
                            correlationId: msg.properties.correlationId
                        });

                        channel.ack(msg);
                    }
                });
            });
        });

        // ticket_unreserved queue
        rabbitMQ.connection.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }
            var queue = 'ticket_unreserved';

            channel.assertQueue(queue, {
                durable: false
            });
            channel.prefetch(1);
            console.log(' [x] Awaiting ticket_unreserved');

            channel.consume(queue, function reply(msg) {
                var dataReceived = JSON.parse(msg.content);
                console.log("Received message for ticket reservation removal: ", dataReceived);

                Ticket.unreserve(dataReceived.other, dataReceived.internalId, err => {
                    if (err) {
                        console.log("Réponse envoyée")
                        channel.sendToQueue(msg.properties.replyTo,
                            Buffer.from("error"), {
                            correlationId: msg.properties.correlationId
                        });

                        channel.ack(msg);
                    } else {
                        console.log("Réponse envoyée")
                        channel.sendToQueue(msg.properties.replyTo,
                            Buffer.from("available"), {
                            correlationId: msg.properties.correlationId
                        });

                        channel.ack(msg);
                    }
                });
            });
        });

        rabbitMQ.connection.createChannel(function (err, channel) {
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
                            case EXCHANGES.Ticket.TicketCreated:
                                ReadStorage.createTicket(dataReceived.internalId);
                                break;
                            case EXCHANGES.Ticket.TicketSetName:
                                ReadStorage.setTicketName(dataReceived.internalId, dataReceived.other);
                                break;
                            case EXCHANGES.Ticket.TicketSetQuantity:
                                ReadStorage.setTicketQty(dataReceived.internalId, dataReceived.other);
                                break;
                            case EXCHANGES.Ticket.TicketSetAvailability:
                                ReadStorage.setTicketAvailability(dataReceived.internalId, dataReceived.other);
                                break;
                            case EXCHANGES.Ticket.TicketSetPrice:
                                ReadStorage.setTicketPrice(dataReceived.internalId, dataReceived.other);
                                break;
                            case EXCHANGES.Payment.PaymentAccepted:
                                Ticket.markCartContentSold(dataReceived.other);
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
        .then(() => {
            console.log("Initialization finished with success");
            /*Ticket.getClientCart('clientId-clientId-clientId-clientId')
                .then(cart => {
                    console.log(cart)
                })
                .catch(err => console.error(err))*/
        })
        .catch(error => console.error("An error happened while configuring :", error));
}, 10000);