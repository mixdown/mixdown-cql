var _ = require('lodash');
var Client = require('node-cassandra-cql').Client;

var CassandraPlugin = function() {};

CassandraPlugin.prototype.attach = function(options) {
  this.consumer = this.consumer || {};

  var cassandra = {
    options: options || {
      connection: {}
    },
    client: null,

    // super generic execute relay.  
    // TODO: determine what queries should be called and move them to proper api calls.
    execute: function(query, args, consistency, callback) {
      var cqlClient = cassandra.client;

      if (!cqlClient) {
        var cb = arguments.length === 3 ? consistency : callback;
        cb(new Error('Cassandra client not initialized.  Did you initialize this plugin?'));
        return;
      }

      cqlClient.execute.apply(cqlClient, arguments);

    }
  };

  this.consumer.cassandra = cassandra;
};

CassandraPlugin.prototype.detach = function(options) {
  var client = this.consumer.cassandra.client;

  if (client) {
    client.shutdown();
  }
};

CassandraPlugin.prototype.init = function(done) {
  var cassandra = this.consumer.cassandra;
  var options = cassandra.options;
  
  options.connection = _.defaults(options.connection, {
    hosts: ['localhost:9042'],
    keyspace: 'system'
  });

// console.log(require('util').inspect(options));

  // https://github.com/jorgebay/node-cassandra-cql#using-it
 //  Client() accepts an objects with these slots:
 //  hosts : String list in host:port format. Port is optional (defaults to 9042).
 //  keyspace : Name of keyspace to use.
 //  username : User for authentication (optional).
 //  password : Password for authentication (optional).
 //  version : Currently only '3.0.0' is supported (optional).
 //  staleTime : Time in milliseconds before trying to reconnect(optional).

  var cqlClient = new Client(options.connection);
  cqlClient.connect(function(err) {
    if (!err) {
      cassandra.client = cqlClient;
    }

    done(err);
  });
};

module.exports = CassandraPlugin;