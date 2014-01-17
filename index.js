var _ = require('lodash');
var cql = require('node-cassandra-cql');
var Client = cql.Client;

var CassandraPlugin = function(namespace) {
  namespace = namespace || 'cassandraClient';
  var client = null;
  var options = null;

  this.attach = function(opt) {
    options = _.defaults(opt || {}, {
      consistency: cql.types.consistencies.quorum,
      connection: {}
    });

    this[namespace] = client = {
      pool: null,

      // super generic execute relay.
      // TODO: determine what queries should be called and move them to proper api calls.
      cql: function(query, args, consistency, callback) {
        var pool = client.pool;

        if (arguments.length === 3) {
          callback = consistency;
          consistency = options.consistency;
        }

        if (!pool) {
          callback(new Error('Cassandra client not initialized.  Did you initialize this plugin?'));
          return;
        }

        pool.execute.call(pool, query, args, consistency, function(err, data) {
          var results = null;

          if (!err && data) {
            var columnMeta = data.meta ? data.meta.columns : [];
            results = {
              raw: data,
              rows: _.map(data.rows, function(row) {
                // convery array row into object row.
                var formattedRow = {};

                // column meta data tells us what position each value lives and what to name it.
                columnMeta.forEach(function(col, i) {
                  formattedRow[col.name] = row[col.name];
                });

                // now return the objectified row.
                return formattedRow;
              })

            };
          }

          callback(err, results);
        });
      }
    };
  };

  this.detach = function(options) {

    // already detached?
    if (!client) {
      return;
    };

    if (client.pool) {
      client.pool.shutdown();
    }
    client = null;

  };

  this.init = function(done) {

    if (!client) {
      done(new Error('Cannot init plugin because cql client is not attached.'));
      return;
    }

    options.connection = _.defaults(options.connection, {
      hosts: ['localhost:9042'],
      keyspace: 'system'
    });

   //  https://github.com/jorgebay/node-cassandra-cql#using-it
   //  Client() accepts an objects with these slots:
   //  hosts : String list in host:port format. Port is optional (defaults to 9042).
   //  keyspace : Name of keyspace to use.
   //  username : User for authentication (optional).
   //  password : Password for authentication (optional).
   //  version : Currently only '3.0.0' is supported (optional).
   //  staleTime : Time in milliseconds before trying to reconnect(optional).

    var pool = new cql.Client(options.connection);

    if(options.debugLog && logger) {
        pool.on('log',function(lvl,msg){
        logger.debug('CQL:' + lvl + '-' + msg);
      });  
    }

    pool.connect(function(err) {
      if (!err) {
        client.pool = pool;
      }

      done(err);
    });
  };
};

module.exports = CassandraPlugin;