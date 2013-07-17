var helenus = require('helenus');

var options = {
  hosts: ['localhost:9160'],
  keyspace: 'unittest',
  user: "cassandra",
  password: "cassandra"
};
var pool = new helenus.ConnectionPool(options);
  
pool.on('error', function(err){
  console.error(err.name, err.message);
});

pool.connect(function(err) {
  console.log(err);
});