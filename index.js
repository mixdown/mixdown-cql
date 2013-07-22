var _ = require('lodash');
var helenus = require('helenus');

var CassandraPlugin = function() {};

CassandraPlugin.prototype.attach = function(options) {
  this.consumer = this.consumer || {};

  var cassandra = {
    options: options || {
      connection: {}
    },
    pool: null,

    // Execute CQL against the db.
    cql: function(query, args, callback) {
      var pool = cassandra.pool;

      if (!pool) {
        callback(new Error('Cassandra connection pool not initialized.  Did you initialize this plugin?'));
        return;
      }

      if (arguments.length === 2 && typeof(args) === 'function') {
        callback = args;
        args = null;
      }

      pool.cql(query, args, function(err, res) {
        var results = res;

        if (results) {
          results = {
            raw: results,
            rows: _.map(results, function(row) {
              var obj = {};
              _.each(row._map, function(i, k) {
                obj[k] = row[i].value;
              });
              return obj;
            })
          };
        }

        callback(err, results);
      });

    },

    thrift: { /* TODO: implement thrift access methods */ }
  };

  this.consumer.cassandra = cassandra;
};

CassandraPlugin.prototype.detach = function(options) {
  var pool = this.consumer.cassandra.pool;

  if (pool) {
    pool.close();
    this.consumer.cassandra = {};
  }
};

CassandraPlugin.prototype.init = function(done) {
  var cassandra = this.consumer.cassandra;
  var options = cassandra.options;

  options.connection = _.defaults(options.connection, {
    hosts: ['localhost:9061'],
    keyspace: 'system'
  });

  // https://github.com/simplereach/helenus#cql
  // pool = new helenus.ConnectionPool({
  //   hosts      : ['localhost:9160'],
  //   keyspace   : 'helenus_test',
  //   user       : 'test',
  //   password   : 'test1233',
  //   timeout    : 3000
  //   //cqlVersion : '3.0.0' // specify this if you're using Cassandra 1.1 and want to use CQL 3
  // });
  var pool = new helenus.ConnectionPool(options.connection);
  
  pool.on('error', function(err){
    // TODO: remove this before committing and decide how to bubble up from the plugin.
    console.error(err.name, err.message);
  });

  pool.connect(function(err) {
    if (!err) {
      cassandra.pool = pool;
    }

    done(err);
  });
};

module.exports = CassandraPlugin;