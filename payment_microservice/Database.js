const EXCHANGES = require('./Exchanges');
const STATUS = require('./Status');

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
            clientId VARCHAR(36),
            amount INT(6),
            status VARCHAR(10) DEFAULT '${STATUS.Pending}',
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
    constructor(connection, dbName, tableName) {
        this.connection = connection;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    initialize() {
        return new Promise((resolve, reject) => {
            createDatabase(this.connection, this.dbName)
                .then(() => useDatabase(this.connection, this.dbName))
                .then(() => createEventStoreTable(this.connection, this.tableName))
                .then(resolve)
                .catch(reject);
        });
    }

    addToEventStore(data) {
        if (data.operation == EXCHANGES.Payment.PaymentGenerated) {
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
    constructor(connection, dbName, tableName) {
        this.connection = connection;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    initialize() {
        return new Promise((resolve, reject) => {
            createDatabase(this.connection, this.dbName)
                .then(() => useDatabase(this.connection, this.dbName))
                .then(() => createReadStorageTable(this.connection, this.tableName))
                .then(resolve)
                .catch(reject);
        });
    }

    createPayment(id) {
        this.connection.query(`INSERT INTO ${this.tableName}(internalId) VALUES ('${id}')`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    setPaymentAmount(internalId, name) {
        this.connection.query(`UPDATE ${this.tableName} SET amount = '${name}' WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    setPaymentClientId(internalId, clientId) {
        this.connection.query(`UPDATE ${this.tableName} SET clientId = '${clientId}' WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    setPaymentStatus(internalId, status) {
        this.connection.query(`UPDATE ${this.tableName} SET status = '${status}' WHERE internalId = '${internalId}'`, (err, res, fields) => {
            if (err) throw err;
        })
    }

    getAllPayments(callback = () => { }) {
        this.connection.query(`select * from ${this.tableName}`, (err, res) => {
            if (err) throw err;
            else
                callback(res);
        });
    }

    getPayment(paymentId, callback = () => { }) {
        this.connection.query(`select * from ${this.tableName} where internalId = '${paymentId}'`, (err, res) => {
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