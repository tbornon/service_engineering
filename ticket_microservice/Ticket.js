const uuid = require('uuid').v4;
const EXCHANGE_NAME = "Ticket"
const EXCHANGES = require('./Exchanges')

function Ticket() {
    this.queueConnection = null;
    this.channelConnection = null;
    this.sqlConnection = null;
}

/**
 * Store RabiitMQ connection inside the object
 */
Ticket.prototype.setQueueConnection = connection => {
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

    console.log("Ticket.js connected to queue")
}

/**
 * Store MySQL connection inside the object
 */
Ticket.prototype.setSqlConnection = connection => {
    this.sqlConnection = connection;

    console.log("Ticket.js connected to database")
}

/**
 * Create a new ticket
 * @param {object} data
 * @param {string} data.name Name of the ticket
 * @param {number} data.quantity Quantity of the ticket
 * @param {number} data.price Price of the ticket
 * @param {function} [callback] Optional callback when the ticket is created. Passing err (if one) and and ticket id (if no error) 
 */
Ticket.prototype.create = (data, callback = () => { }) => {
    if (data.name && data.quantity && data.price) {
        let ticketUuid = uuid();
        // Publish TicketCreated
        publish(this.channelConnection, ticketUuid, null, EXCHANGES.Ticket.TicketCreated);
        // Publish TicketSetName
        publish(this.channelConnection, ticketUuid, data.name, EXCHANGES.Ticket.TicketSetName);
        // Publish TicketSetQuantity
        publish(this.channelConnection, ticketUuid, data.quantity, EXCHANGES.Ticket.TicketSetQuantity);
        // Publish TicketSetAvailability
        publish(this.channelConnection, ticketUuid, data.quantity, EXCHANGES.Ticket.TicketSetAvailability);
        // Publish TicketSetPrice
        publish(this.channelConnection, ticketUuid, data.price, EXCHANGES.Ticket.TicketSetPrice);

        callback(null, ticketUuid);
    } else
        callback(new Error("MissingParameter"));

}

/**
 * Reserve a ticket for a client
 * @param {string} ticketId Id of the ticket to reserve
 * @param {string} clientId Id of the client to reserve for
 * @param {function} [callback] Optional callback when the ticket is reserved. Passing err (if one)
 */
Ticket.prototype.reserve = (ticketId, clientId, callback = () => { }) => {
    let self = this;
    // Returns the availability for a given ticket
    this.sqlConnection.query(`
    SELECT other as availability
    FROM (
        -- Select all ticket with the corresponding internal id
        SELECT *
        FROM EventStore
        WHERE operation = '${EXCHANGES.Ticket.TicketSetAvailability}' and internalId = '${ticketId}'
        --
    ) as filtered
    -- Filter rows and only select the last update of TicketSetAvailability
    WHERE 
        filtered.aid = (
            SELECT max(aid)
            FROM EventStore
            WHERE internalId = '${ticketId}' and operation = '${EXCHANGES.Ticket.TicketSetAvailability}'
        ) 
    `, (err, result, fields) => {
        if (err) throw err;
        // If ticket is found
        if (result.length > 0) {
            let availability = parseInt(result[0].availability);
            // If the ticket is available
            if (availability > 0) {
                publish(self.channelConnection, ticketId, clientId, EXCHANGES.Ticket.TicketReserved);
                publish(self.channelConnection, ticketId, availability - 1, EXCHANGES.Ticket.TicketSetAvailability);
                callback();
            }
            else {
                callback(new Error("No ticket available"));
            }
        }
        else {
            callback(new Error("Ticket doesn't exist"))
        }
    })
}

/**
 * Remove reservation of a ticket for a client
 * @param {string} ticketId Id of the ticket to remove reservation
 * @param {string} clientId Id of the client to remove reservation from
 * @param {function} [callback] Optional callback when the ticket is reserved. Passing err (if one) 
 */
Ticket.prototype.unreserve = (ticketId, clientId, callback = () => { }) => {
    let self = this;
    // Get the number of available ticket
    this.sqlConnection.query(`
    SELECT other as availability
    FROM (
        -- Select all ticket with the corresponding internal id
        SELECT *
        FROM EventStore
        WHERE operation = '${EXCHANGES.Ticket.TicketSetAvailability}' and internalId = '${ticketId}'
        --
    ) as filtered
    -- Filter rows and only select the last update of TicketSetAvailability
    WHERE 
        filtered.aid = (
            SELECT max(aid)
            FROM EventStore
            WHERE internalId = '${ticketId}' and operation = '${EXCHANGES.Ticket.TicketSetAvailability}'
        ) 
    `, (err, result, fields) => {
        if (err) throw err;
        // If ticket id is found
        if (result.length > 0) {
            let availability = parseInt(result[0].availability);
            publish(self.channelConnection, ticketId, clientId, EXCHANGES.Ticket.TicketUnreserved);
            // Increment available quantity in event store
            publish(self.channelConnection, ticketId, availability + 1, EXCHANGES.Ticket.TicketSetAvailability);
            callback();
        }
        else {
            callback(new Error("Ticket doesn't exist"))
        }
    });
}

/**
 * Order ticket(s) for a client
 * @param {string} clientId Id of the client who wants to order
 * @param {function} [callback] Optional callback when the card is ordered. Passing err (if one) and payment (if no error)
 */
Ticket.prototype.order = (clientId, callback = () => { }) => {
    // Get total price for everything that is in client cart
    this.sqlConnection.query(`select sum(other) as price
    from (
        select main.internalId as internalId
        from (
            select internalId, max(aid) as maxAid
            from EventStore
            where other = '${clientId}'
            group by internalId
        ) as main
        join EventStore as es on main.internalId = es.internalId and main.maxAid = es.aid
        where operation = '${EXCHANGES.Ticket.TicketReserved}'
    ) as xy
    join EventStore as es2 on xy.internalId = es2.internalId and es2.operation = '${EXCHANGES.Ticket.TicketSetPrice}'`, (err, result) => {
        if (err) throw err;
        else {
            console.log("order : ",result);
            console.log('client id: ', clientId)


            getClientCart(this.sqlConnection, clientId).then(data => {console.log(data)})
            // If client exists / client cart isn't empty
            if (result.length > 0) {
                let amount = result[0].price;

                if (amount != undefined) {
                    this.queueConnection.createChannel(function (err, channel) {
                        if (err) {
                            throw err;
                        }

                        // Create a queue for the response from payment microservice
                        channel.assertQueue('', {
                            exclusive: true
                        }, function (err, q) {
                            if (err) {
                                throw err;
                            }
                            var correlationId = uuid();
                            var requestData = {
                                amount: amount,
                                clientId: clientId
                            }

                            console.log(' [x] Requesting payment generation', requestData);

                            // Response of the payment microservice handler
                            channel.consume(q.queue, function (msg) {
                                if (msg.properties.correlationId == correlationId) {
                                    let receivedData = JSON.parse(msg.content);
                                    callback(null, receivedData.paymentId);
                                    console.log(' [.] Got payment id %s for client %s', receivedData.paymentId, clientId);
                                }
                            }, {
                                noAck: true
                            });

                            // Send RPC to payment microservice
                            channel.sendToQueue('payment_generate',
                                Buffer.from(JSON.stringify(requestData)), {
                                correlationId: correlationId,
                                replyTo: q.queue
                            });
                        });
                    });
                }
            } else {
                callback(new Error('No item in cart'))
            }
        }
    });
}

/**
 * Return a ticket
 * @param {object} data
 * @param {string} data.ticketId Id of the ticket to refund
 * @param {string} data.clientId Client to refund to
 * @param {function} [callback] Optional callback when the ticket is refunded. Passing err (if one) and refund id (if no error)
 */
Ticket.prototype.return = (data, callback) => {
    if (data.ticketId && data.clientId) {
        let self = this;
        this.sqlConnection.query(`select other as price 
        from EventStore
        where operation = '${EXCHANGES.Ticket.TicketSetPrice}' and internalId = '${data.ticketId}'`
            , (err, result) => {
                if (err) throw err;
                let amount = result[0].price;

                publish(this.channelConnection, data.ticketId, data.clientId, EXCHANGES.Ticket.TicketReceivedForRefund);
                this.queueConnection.createChannel(function (err, channel) {
                    if (err) {
                        throw err;
                    }

                    channel.assertQueue('', {
                        exclusive: true
                    }, function (err, q) {
                        if (err) {
                            throw err;
                        }
                        var correlationId = uuid();
                        var requestData = {
                            amount: amount,
                            clientId: data.clientId
                        }

                        console.log(' [x] Requesting payment refunding', requestData);

                        channel.consume(q.queue, function (msg) {
                            if (msg.properties.correlationId == correlationId) {
                                let receivedData = JSON.parse(msg.content);
                                publish(self.channelConnection,
                                    data.ticketId,
                                    receivedData.paymentId,
                                    EXCHANGES.Ticket.TicketRefunded
                                );
                                console.log(' [.] Got payment refund id %s for client %s', receivedData.paymentId, data.clientId);

                                increaseTicketAvailability(self.sqlConnection, self.channelConnection, data.ticketId)
                                    .then(() => callback(receivedData.paymentId))
                                    .catch(callback);
                            }
                        }, {
                            noAck: true
                        });

                        channel.sendToQueue('payment_refund',
                            Buffer.from(JSON.stringify(requestData)), {
                            correlationId: correlationId,
                            replyTo: q.queue
                        });
                    });
                });
            });
    }
}

Ticket.prototype.getClientCart = clientId => {
    return getClientCart(this.sqlConnection, clientId);
}

Ticket.prototype.markCartContentSold = clientId => {
    return new Promise((resolve, reject) => {
        getClientCart(this.sqlConnection, clientId)
            .then(cart => {
                cart.forEach(ticket => {
                    publish(this.channelConnection, ticket, clientId, EXCHANGES.Ticket.TicketSold);
                });

                resolve();
            })
            .catch(reject);
    });
}

/**
 * Publish a new event to the messaging system
 * @param {object} channel Connection to RabbitMQ channel
 * @param {string} internalId Id of the ticket concerned by the event
 * @param {string} [other] Optional - Option other information
 * @param {string} operation Event's operation/action
 */
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

let getClientCart = (sqlConnection, clientId) => {
    return new Promise((resolve, reject) => {
        sqlConnection.query(`
        select main.internalId as internalId
        from (
            select internalId, max(aid) as maxAid
            from EventStore
            where other = '${clientId}' and operation in ('TicketReserved', 'TicketSold', 'TicketUnreserved')
            group by internalId
        ) as main
        join EventStore as es on main.internalId = es.internalId and main.maxAid = es.aid
        where operation = '${EXCHANGES.Ticket.TicketReserved}'`, (err, result) => {
            if (err) return reject(err);
            resolve(result.map(x => x.internalId));
        });
    });
}

let increaseTicketAvailability = (sqlConnection, channelConnection, ticketId) => {
    return new Promise((resolve, reject) => {
        sqlConnection.query(`
    SELECT other as availability
    FROM (
        -- Select all ticket with the corresponding internal id
        SELECT *
        FROM EventStore
        WHERE operation = '${EXCHANGES.Ticket.TicketSetAvailability}' and internalId = '${ticketId}'
        --
    ) as filtered
    -- Filter rows and only select the last update of TicketSetAvailability
    WHERE 
        filtered.aid = (
            SELECT max(aid)
            FROM EventStore
            WHERE internalId = '${ticketId}' and operation = '${EXCHANGES.Ticket.TicketSetAvailability}'
        ) 
    `, (err, result, fields) => {
            if (err) throw err;
            // If ticket id is found
            if (result.length > 0) {
                let availability = parseInt(result[0].availability);
                // Increment available quantity in event store
                publish(channelConnection, ticketId, availability + 1, EXCHANGES.Ticket.TicketSetAvailability);
                resolve();
            }
            else {
                reject(new Error("Ticket doesn't exist"))
            }
        });
    });
}

module.exports = new Ticket()