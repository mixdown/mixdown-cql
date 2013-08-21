var _ = require('lodash');
var Client = require('node-cassandra-cql').Client;

var CassandraPlugin = function() {};

CassandraPlugin.prototype.attach = function(options) {
  this.cassandraClient = {};

  var cassandraClient = {
    options: options || {
      connection: {}
    },
    pool: null,

    // super generic execute relay.  
    // TODO: determine what queries should be called and move them to proper api calls.
    cql: function(query, args, consistency, callback) {
      var pool = cassandraClient.pool;

      if (!pool) {
        var cb = arguments.length === 3 ? consistency : callback;
        cb(new Error('Cassandra client not initialized.  Did you initialize this plugin?'));
        return;
      }

      pool.execute.apply(pool, arguments);

    }
  };

  this.cassandraClient = cassandraClient;
};

CassandraPlugin.prototype.detach = function(options) {
  var client = this.cassandraClient.pool;

  if (pool) {
    pool.shutdown();
  }
};

CassandraPlugin.prototype.init = function(done) {
  var cassandraClient = this.cassandraClient;
  var options = cassandraClient.options;
  
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

  var pool = new Client(options.connection);
  pool.connect(function(err) {
    if (!err) {
      cassandraClient.pool = pool;
    }

    done(err);
  });
};

module.exports = CassandraPlugin;