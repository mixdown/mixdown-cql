var test = require('tape').test;
var broadway = require('broadway');
var CassandraPlugin = require('../../index.js');
var util = require('util');
var Pipeline = require('node-pipeline');

var pluginOptions = {
  connection: {
    hosts: ['localhost:9160'],
    keyspace: 'system',
    user: "cassandra",
    password: "cassandra",
    hostPoolSize: 5
  }
};

test('Connect to cassandra', function(t) {
  t.plan(1);

  var app = new broadway.App();
  var plugin = new CassandraPlugin();
  app.use(plugin, pluginOptions);

  app.init(function(err) {

    if (err) {
      console.log(util.inspect(err));
    }

    t.notOk(err, 'Should not return error');
    app.remove(plugin); // detach.
    t.end();
  });

});

test('Create keyspace, Insert data, Run query', function(t) {
  t.plan(4);

  var app = new broadway.App();
  var testval = JSON.stringify({"foo":"bar","bar":"baz"});
  var options =  {
    connection: {
      keyspace: 'unittest',
      hosts: pluginOptions.connection.hosts,
      user: pluginOptions.connection.user,
      password: pluginOptions.connection.password,
      hostPoolSize: pluginOptions.connection.hostPoolSize
    }
  };

  app.use(new CassandraPlugin(), options);

  app.init(function(err) {
    if (err) {
      console.log(util.inspect(err));
    }

    t.notOk(err, 'Init should not return error');

    app.consumer.cassandra.pool.on('error', function(err) {
      console.log(util.inspect(err));
    });

    var plCreate = Pipeline.create('Create table and insert data test');
    plCreate.on('error', function() {});

    plCreate.on('step', function(data, action) {
      console.log([data.pipeline.name, data.step].join(': '));
    });

    plCreate.on('end', function(err, results) {
      if (err) {
        console.log(util.inspect(err));
      }

      t.notOk(err, 'Create keyspace, insert data should not return error');

      app.consumer.cassandra.cql('SELECT * FROM testdata', [], function(err, selectResults) {
        
        if (err) {
          console.log(util.inspect(err));
        }

        console.log(util.inspect(selectResults));

        t.notOk(err, 'Select data should not return error');
        t.equal(selectResults.rows.length, 3, 'Should return correct number of row results.');
        t.end();
      });
    });

    // plCreate.use(function(results, next) {
    //   app.consumer.cassandra.cql('DROP KEYSPACE unittest', [], function() { next(); });
    // }, 'drop old keyspace');

    // plCreate.use(function(results, next) {
    //   app.consumer.cassandra.cql(
    //   'CREATE KEYSPACE re WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\': \'1\'}'
    //   ,[], next);
    // }, 'create new keyspace');

    plCreate.use(function(results, next) {
      app.consumer.cassandra.cql(
      'DROP TABLE testdata' 
      ,[], function() { next() });
    }, 'drop old table');

    plCreate.use(function(results, next) {
      app.consumer.cassandra.cql(
      'CREATE TABLE testdata (id double PRIMARY KEY, col1 text)' 
      ,[], next);
    }, 'create new table');

    plCreate.use(function(results, next) {
      app.consumer.cassandra.cql(
      'BEGIN BATCH ' + 
      'INSERT INTO testdata (id, col1) VALUES (-1234, ?) ' +
      'INSERT INTO testdata (id, col1) VALUES (1234, ?) ' +
      'INSERT INTO testdata (id, col1) VALUES (3456, ?) ' + 
      'APPLY BATCH'
      ,[testval, testval, testval], next);
    }, 'insert data to table');

    plCreate.execute();

  });

});