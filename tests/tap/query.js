var test = require('tape').test;
var broadway = require('broadway');
var CassandraPlugin = require('../../index.js');
var util = require('util');
var Pipeline = require('node-pipeline');

var pluginOptions = {
  connection: {
    hosts: ['localhost:9042'],
    keyspace: 'system'
  }
};

test('Connect to cassandra', function(t) {
  t.plan(1);

  var app = new broadway.App();
  app.use(new CassandraPlugin(), pluginOptions);

  app.init(function(err) {

    if (err) {
      console.log(util.inspect(err));
    }

    t.notOk(err, 'Should not return error');
    t.end();
  });

});

test('Create keyspace, Insert data, Run query', function(t) {
  t.plan(4);

  var app = new broadway.App();
  var testval = JSON.stringify({"foo":"bar","bar":"baz"});
  app.use(new CassandraPlugin(), pluginOptions);

  app.init(function(err) {
    if (err) {
      console.log(util.inspect(err));
    }

    t.notOk(err, 'Init should not return error');

    app.consumer.cassandra.client.on('log', function(level, msg) {
      console.log(level + ": " + msg);
    });

    var plCreate = Pipeline.create();
    plCreate.on('error', function() {});
    plCreate.on('end', function(err, results) {
      if (err) {
        console.log(util.inspect(err));
      }

      t.notOk(err, 'Create keyspace, insert data should not return error');

      app.consumer.cassandra.execute('SELECT * FROM unittest.testdata', [], function(err, selectResults) {
        
        if (err) {
          console.log(util.inspect(err));
        }

        console.log(util.inspect(selectResults));

        t.notOk(err, 'Select data should not return error');
        t.equal(selectResults.rows.length, 3, 'Should return correct number of row results.');
        t.end();
      });
    });

    plCreate.use(function(results, next) {
      app.consumer.cassandra.execute('DROP KEYSPACE unittest', [], function() { next(); });
    });

    plCreate.use(function(results, next) {
      app.consumer.cassandra.execute(
      'CREATE KEYSPACE unittest WITH replication = {\'class\': \'SimpleStrategy\', \'replication_factor\' : 1}'
      ,[], next);
    });

    plCreate.use(function(results, next) {
      app.consumer.cassandra.execute(
      'CREATE TABLE unittest.testdata (id double PRIMARY KEY, col1 text)' 
      ,[], next);
    });

    plCreate.use(function(results, next) {
      app.consumer.cassandra.execute(
      'BEGIN BATCH ' + 
      'INSERT INTO unittest.testdata (id, col1) VALUES (-1234, ?) ' +
      'INSERT INTO unittest.testdata (id, col1) VALUES (1234, ?) ' +
      'INSERT INTO unittest.testdata (id, col1) VALUES (3456, ?) ' + 
      'APPLY BATCH'
      ,[testval, testval, testval], next);
    });

    plCreate.execute();

  });

});