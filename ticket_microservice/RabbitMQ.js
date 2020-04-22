const amqp = require('amqplib/callback_api');

/**
 * Class for managing RabbitMQ connection
 */
class RabbitMQ {
    /**
     * Initialize RabbitMQ object
     * @param {string} url Url of the RabbitMQ server
     */
    constructor(url) {
        this.connection = null;
        this.channel = null;
        this.url = url;
    }

    /**
     * Connect to RabbitMQ
     */
    connect() {
        return new Promise((resolve, reject) => {
            amqp.connect(this.url, function (err, connection) {
                if (err) return reject(err);
                console.log("Connected to RabbitMQ")

                resolve(connection);
            });
        })
    }

    /**
     * Initialize object
     *   - Connect to RabbitMQ
     *   - Store RabbitMQ's connection in object
     */
    initialize() {
        return new Promise((resolve, reject) => {
            this.connect()
                .then(connection => {
                    this.connection = connection;
                    resolve();
                })
                .catch(reject);
        });
    }
}

module.exports = RabbitMQ;