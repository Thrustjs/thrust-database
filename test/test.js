var coverage = require('coverage');
var testDatabases = require('./test.database.js');

coverage.init();

var failuresCount = testDatabases(['postgresql', 'sqlite']);

let coverageAverage = coverage.report();

exit(failuresCount);