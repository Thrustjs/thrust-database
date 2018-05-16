var testDatabases = require('./test.database.js');

var failuresCount = testDatabases(['postgresql', 'sqlite']);
exit(failuresCount);