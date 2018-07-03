var coverage = require('coverage');
var testDatabases = require('./test.database.js');

const MIN_COVERAGE = 94;

coverage.init({
    ignore: [
        '../dist/dbv1.js'
    ]
});

let failuresCount = testDatabases(['postgresql', 'sqlite']);

let coverageAverage = coverage.report();

if (failuresCount == 0) {
    if (coverageAverage < MIN_COVERAGE) {
        console.log('Cobertura menor que o permitido: ', coverageAverage, '[min: ' + MIN_COVERAGE + ']')
        exit(1);
    }
} else {
    exit(failuresCount);
}