var testDatabase = require('./test.database.js');

var databases = ['postgresql', 'sqlite'];

var failures = databases.reduce(function(sum, rdbms){
    return sum + testDatabase(rdbms);
}, 0);

exit(failures);