const amqp = require('amqplib/callback_api');

class RabbitMQ {
    constructor(url) {
        this.connection = null;
        this.channel = null;
        this.url = url;
    }

    connect() {
        return new Promise((resolve, reject) => {
            amqp.connect(this.url, function (err, connection) {
                if (err) return reject(err);
                console.log("Connected to RabbitMQ")

                resolve(connection);
            });
        })
    }

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