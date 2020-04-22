const EXCHANGES = require('./Exchanges');

/**
 * Create (if it doesn`t exists) a database
 * @param {Object} connection Connection to database
 * @param {string} dbName Name of the database to create
 * @returns {Promise} Promise of creation 
 */
function createDatabase(connection, dbName) {
    return new Promise((resolve, reject) => {
        connection.query(`CREATE DATABASE IF NOT EXISTS ${dbName}`, function (err, result) {
            if (err) return reject(err);
            console.log(`Database ${dbName} created`);
            resolve();
        });
    });
}

/**
 * Use a database
 * @param {Object} connection Connection to database
 * @param {string} dbName Database to use
 * @returns {Promise} Promise of use 
 */
function useDatabase(connection, dbName) {
    return new Promise((resolve, reject) => {
        connection.query(`USE ${dbName}`, (err, result) => {
            if (err) return reject(err);
            console.log(`Using ${dbName}`)
            resolve();
        });
    });
}

/**
 * Create (if it doesn't exists) the table for EventStore
 * @param {Object} connection Connection to database
 * @param {string} tableName Name of the table to create
 * @returns {Promise} Promise of create
 */
function createEventStoreTable(connection, tableName) {
    return new Promise((resolve, reject) => {
        connection.query(`CREATE TABLE IF NOT EXISTS ${tableName} (
            uid INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            aid INT(6) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            internalId VARCHAR(36) NOT NULL,
            other VARCHAR(36),
            operation VARCHAR(30) NOT NULL
            )`,
            function (error, results, fields) {
                if (error) return reject(err);
                console.log(`Table ${tableName} created`);
                resolve();
            }
        );
    });
}

/**
 * Create (if it doesn't exists) the table for ReadStorage
 * @param {Object} connection Connection to database
 * @param {string} tableName Name of the table to create
 * @returns {Promise} Promise of create
 */
function createReadStorageTable(connection, tableName) {
    return new Promise((resolve, reject) => {
        connection.query(`CREATE TABLE IF NOT EXISTS ${tableName} (
            uid INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,
            internalId VARCHAR(36),
            name VARCHAR(30),
            price INT(6),
            quantity INT(6),
            availability INT(6),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )`,
            function (error, results, fields) {
                if (error) return reject(err);
                console.log(`Table ${tableName} created`);
                resolve();
            }
        );
    });
}


/**
 * Instanciate a new EventStore database
 * @param {Object} connection Connection to the database
 * @param {string} dbName Name of the db
 * @param {string} tableName Name of the table
 */
class EventStoreDB {
    /**
     * Create EventStoreDB object
     * @param {Object} connection Connection to a mysql server
     * @param {string} dbName Name of the database to use/create
     * @param {string} tableName Name of the table to use/create
     */
    constructor(connection, dbName, tableName) {
        this.connection = connection;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    /**
     * Initialize EventStoreDB
     *   - Create the database if it doesn't exit
     *   - Use the database
     *   - Create the table if it doesn't exist
     */
    initialize() {
        return new Promise((resolve, reject) => {
            createDatabase(this.connection, this.dbName)
                .then(() => useDatabase(this.connection, this.dbName))
                .then(() => createEventStoreTable(this.connection, this.tableName))
                .then(resolve)
                .catch(reject);
        });
    }

    /**
     * Add event to the event store
     * @param {object} data Event data to store
     * @param {string} data.internalId Id of the ticket which is concerned by the event
     * @param {string} data.operation Type of event operation
     * @param {string} [data.other] Optional - Other data related to the event
     */
    addToEventStore(data) {
        if (data.operation == EXCHANGES.Ticket.TicketCreated || data.operation == EXCHANGES.Payment.PaymentAccepted) {
            this.connection.query(`INSERT INTO ${this.tableName} (aid, internalId, other, operation)
            VALUES (1, '${data.internalId}', null, '${data.operation}')`,
                function (err, results, fields) {
                    if (err)
                        throw err;
                });
        } else {
            this.connection.query(`INSERT INTO ${this.tableName} (aid, internalId, other, operation)
            SELECT (max(aid)+1) as aid, '${data.internalId}', ${data.other != undefined ? "'" + data.other + "'" : null}, '${data.operation}'
            FROM ${this.tableName}
            WHERE internalId = '${data.internalId}'`,
                function (error, results, fields) {
                    if (error) throw error;
                }
            );
        }
    }
}

/**
 * Instanciate a new ReadStorage database
 * @param {Object} connection Connection to the database
 * @param {string} dbName Name of the db
 * @param {string} tableName Name of the table
 */
class ReadStorageDB {
    /**
     * Create ReadStorageDB object
     * @param {Object} connection Connection to a mysql server
     * @param {string} dbName Name of the database to use/create
     * @param {string} tableName Name of the table to use/create
     */
    constructor(connection, dbName, tableName) {
        this.connection = connection;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    /**
     * Initialize ReadStorageDB
     *   - Create the database if it doesn't exit
     *   - Use the database
     *   - Create the table if it doesn't exist
     */
    initialize() {
        return new Promise((resolve, reject) => {
            createDatabase(this.connection, this.dbName)
                .then(() => useDatabase(this.connection, this.dbName))
                .then(() => createReadStorageTable(this.connection, this.tableName))
                .then(resolve)
                .catch(reject);
        });
    }

    /**
     * Creates a new ticket
     * @param {string} id Id of the new ticket to create
     */
    createTicket(id) {
        this.connection.query(`INSERT INTO ${this.tableName}(internalId) VALUES ('${id}')`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    /**
     * Update ticket name
     * @param {string} internalId Id of the ticket to update
     * @param {string} name New name of the ticket
     */
    setTicketName(internalId, name) {
        this.connection.query(`UPDATE ${this.tableName} SET name = '${name}' WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    /**
     * Update ticket quantity
     * @param {string} internalId Id of the ticket to update
     * @param {number} qty New quantity of the ticket
     */
    setTicketQty(internalId, qty) {
        this.connection.query(`UPDATE ${this.tableName} SET quantity = ${qty} WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    /**
     * Update ticket availability
     * @param {string} internalId Id of the ticket to update
     * @param {number} availability New availability of the ticket
     */
    setTicketAvailability(internalId, availability) {
        this.connection.query(`UPDATE ${this.tableName} SET availability = ${availability} WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    /**
     * Update ticket price
     * @param {string} internalId Id of the ticket to update
     * @param {number} price New price of the ticket
     */
    setTicketPrice(internalId, price) {
        this.connection.query(`UPDATE ${this.tableName} SET price = '${price}' WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    /**
     * Get list of tickets
     * @param {function} [callback] Optional - Callback with the result of the request
     */
    getAllTickets(callback = () => { }) {
        this.connection.query(`select * from ${this.tableName}`, (err, res) => {
            if (err) throw err;
            else
                callback(res);
        });
    }

    /**
     * Get informations on a specified ticket
     * @param {string} ticketId Ticket's id
     * @param {function} [callback] Optional - Callback function with result as a parameter
     */
    getTicket(ticketId, callback = () => { }) {
        this.connection.query(`select * from ${this.tableName} where internalId = '${ticketId}'`, (err, res) => {
            if (err) throw err;
            else
                callback(res[0]);
        });
    }
}

module.exports = {
    EventStoreDB,
    ReadStorageDB
}