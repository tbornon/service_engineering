const amqp = require('amqplib/callback_api');
const mysql = require('mysql');
const express = require('express');
const app = express();
const uuid = require('uuid').v4;
const ROUTING_KEYS = require('./RoutingKeys');

var hostname = 'localhost';
var port = 3000;
const EXCHANGE_NAME = "User"



var connection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});

var readStorageConnection = mysql.createConnection({
    host: 'mysql',
    user: 'root',
    password: 'root',
});

function initialisation(callback) {

    connection.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");

        createAndUseDatabase(() => {
            createTable(callback);
        })
    });

    readStorageConnection.connect(function (err) {
        if (err) throw err;
        console.log("Connected!");

        createAndUseStorageDatabase(() => {
            createSorageTable();
        })
    });

    amqp.connect('amqp://rabbitmq', function (err, connection) {
        if (err) throw err;
        console.log("Connected to RabbitMQ")
        subscribeUser(connection);
    });

    app.use(express.urlencoded({ extended: true }));
    app.use(express.json());
    configureRoutes();
    app.listen(port, function () {
        console.log("Mon serveur fonctionne sur http://:" + port);
    });
}

function configureRoutes() {
    app.post('/user/', function (req, res) {
        let id = uuid();
        addToEventStore({
            operation: ROUTING_KEYS.UserGenerated,
            other: req.body.clientName,
            id
        }, (data) => {
            res.json(data);
        });
    });

    //POST
    app.post('/user/cart', function (req, res) {
        addToEventStore({
            operation: ROUTING_KEYS.TicketAddedToCart,
            internalId: req.body.clientId,
            other: req.body.ticketId
        });
        res.send();
    });

    app.delete('/user', function (req, res) {
        addToEventStore({
            operation: ROUTING_KEYS.UserRemoved,
            other: req.body.clientName
        });
        res.send();
    });

    app.delete('/user/cart', function (req, res) {
        addToEventStore({
            operation: ROUTING_KEYS.TicketRemovedFromCart,
            internalId: req.body.clientId,
            other: req.body.ticketId
        });
        res.send();
    })
}

function subscribeUser(connection) {
    connection.createChannel(function (err, channel) {
        if (err)
            throw err;

        channel.assertExchange(EXCHANGE_NAME, 'direct', {
            durable: false
        });

        channel.assertQueue('',
            {
                exclusive: true
            },
            function (err, q) {
                if (err)
                    throw err;

                Object.keys(ROUTING_KEYS).forEach(key => {
                    channel.bindQueue(q.queue, EXCHANGE_NAME, ROUTING_KEYS[key]);
                    console.log("Queue binded to routing key : %s", ROUTING_KEYS[key])
                })

                channel.consume(q.queue, function (msg) {
                    console.log(" [x] %s: '%s'", msg.fields.routingKey, msg.content.toString());
                    let dataReceived = JSON.parse(msg.content);

                    addToEventStore({
                        internalId: dataReceived.option1,
                        other: dataReceived.option2,
                        operation: msg.fields.routingKey
                    })
                }, {
                    noAck: true
                });
            });
    });
}

function createAndUseDatabase(callback) {
    connection.query("CREATE DATABASE IF NOT EXISTS EventStoreDB", function (err, result) {
        if (err) throw err;
        console.log("Database EventStoreDB created");
        connection.query("USE EventStoreDB", (err, result) => {
            if (err) throw err;
            console.log("Using EventStoreDB")
            if (callback !== undefined)
                callback();
        });
    });
}

function createAndUseStorageDatabase(callback) {
    readStorageConnection.query("CREATE DATABASE IF NOT EXISTS StorageDB", function (err, result) {
        if (err) throw err;
        console.log("Database StorageDB created");
        readStorageConnection.query("USE StorageDB", (err, result) => {
            if (err) throw err;
            console.log("Using StorageDB")
            if (callback !== undefined)
                callback();
        });
    });
}

function createTable(callback) {
    connection.query(`CREATE TABLE IF NOT EXISTS EventStore (
    uid INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    aid INT(6) NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    internalId VARCHAR(36) NOT NULL,
    other VARCHAR(36) NOT NULL,
    operation VARCHAR(30) NOT NULL
    );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table EventStore created');
            if (callback != undefined)
                callback();
        }
    );
}

function createSorageTable(callback) {
    readStorageConnection.query(`CREATE TABLE IF NOT EXISTS userStorage (
        clientId VARCHAR(36) NOT NULL,
        name VARCHAR(36) NOT NULL
        );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table userStorage created');
            if (callback != undefined)
                callback();
        }
    );
    readStorageConnection.query(`CREATE TABLE IF NOT EXISTS ticketStorage (
        clientId VARCHAR(36) NOT NULL,
        ticketId VARCHAR(36) NOT NULL
        );`,
        function (error, results, fields) {
            if (error) throw error;
            console.log('Table ticketStorage created');
            if (callback != undefined)
                callback();
        }
    );
}

function isUser(name, callback) {
    readStorageConnection.query(`SELECT EXISTS(
        SELECT *
        FROM userStorage
        WHERE name = '${name}') as isuser`,
        function (error, result, fields) {
            if (error) throw error;
            console.log(result[0].isuser)
            return callback(result[0].isuser);
        });
}

function addUser(id, name) {
    readStorageConnection.query(`INSERT INTO userStorage SELECT '${id}', '${name}'`,
        function (error, results, fields) {
            if (error) throw error;
        });
}

function removeUser(name) {
    readStorageConnection.query(`DELETE FROM userStorage WHERE name = '${name}'`,
        function (error, results, fields) {
            if (error) throw error;
        });
}

function addTicket(id, ticket) {
    console.log("hello");
    readStorageConnection.query(`INSERT INTO ticketStorage VALUES ( '${id}', '${ticket}')`,
        function (error, results, fields) {
            if (error) throw error;
        });
}

function removeTicket(id, ticket) {
    readStorageConnection.query(`DELETE FROM ticketStorage WHERE clientId = '${id}' AND ticketId = '${ticket}'`,
        function (error, results, fields) {
            if (error) throw error;
        });
}

function addToEventStore(data, callback = () => { }) {
    function addToDatabase(aid, internalId, other, operation) {
        connection.query(
            `INSERT INTO EventStore
        (aid, internalId, other, operation)
        SELECT ${aid}, '${internalId}', '${other}', '${operation}'`,
            function (err, results, fields) {
                if (err)
                    throw err;
            });
    }

    if (data.operation == ROUTING_KEYS.UserGenerated) {
        isUser(data.other, function (result) {
            if (result == 0) {
                addToDatabase(1, data.id, data.other, data.operation);
                addUser(data.id, data.other);
                console.log('user ' + data.other + " added");
                callback(data);
            }
            else {
                console.log('user ' + data.other + " already exists");
                callback({ message: "User already exists" });
            }
        });

    } else {
        connection.query(`SELECT max(aid) from EventStore`,
            function (error, results, fields) {
                if (error) throw error;
                let aid = results[0]["max(aid)"];
                if (data.operation == ROUTING_KEYS.UserRemoved) {
                    connection.query("SELECT internalId from EventStore WHERE operation = 'UserGenerated' AND other = '" + data.other + "'", function (err, result, fields) {
                        if (err) throw err;
                        console.log(result[0].internalId);
                        addToDatabase(aid + 1, result[0].internalId, data.other, data.operation);
                        removeUser(data.other, result[0].internalId);
                    });
                }
                else {
                    addToDatabase(aid + 1, data.internalId, data.other, data.operation);

                    if (data.operation == ROUTING_KEYS.TicketAddedToCart) {
                        sendToTicket("ticket_reserved", data, function (result) {
                            console.log(result);

                            if (result == 'available') {
                                addTicket(data.internalId, data.other);
                            }
                            else { console.log("Ticket unavailaible"); }
                        })
                    }
                    if (data.operation == ROUTING_KEYS.TicketRemovedFromCart) {
                        sendToTicket("ticket_unreserved", data);
                        removeTicket(data.internalId, data.other);
                    }
                }
            }
        );
    }
}

function sendToTicket(queueName, data, callback = () => { }) {
    amqp.connect('amqp://rabbitmq', function (error0, connection2) {
        connection2.createChannel(function (error1, channel) {
            if (error1) {
                throw error1;
            }

            var queue = queueName;

            channel.assertQueue('', {
                exclusive: true
            }, function (error2, q) {
                if (error2) {
                    throw error2;
                }

                let correlationId = uuid();

                channel.consume(q.queue, function (msg) {
                    if (msg.properties.correlationId == correlationId) {
                        callback(msg.content.toString());
                    }
                }, {
                    noAck: true
                });

                channel.sendToQueue(
                    queue,
                    Buffer.from(JSON.stringify(data)),
                    {
                        replyTo: q.queue,
                        correlationId: correlationId,
                    }
                );

                console.log(" [x] Sent %s", data);
            });
        });
    });
}

setTimeout(() => {
    initialisation(() => {
        /*addToEventStore({
            operation: ROUTING_KEYS.TicketRemovedFromCart,
            internalId: 33,
            other: '1755d7df-3629-4fb6-89d5-cf29db5f062c'
        })*/
        /*sendToTicket("ticket_reserved", {
            ticketId: "0b62d9b4-6c50-4445-bf47-4b6941796bb4",
            clientId: "test"
        });*/
    });
}, 10000)
