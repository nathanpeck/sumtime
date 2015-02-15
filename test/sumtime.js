var InMemory = require(process.cwd() + '/lib/datastores/memory.js'),
    SumTime  = require(process.cwd() + '/lib/sumtime.js'),
    async    = require('async'),
    expect   = require('chai').expect;

var testStore = new InMemory();
var testSum;

// Helper functions for generating random data for the tests.
function randomDate(start, end) {
  return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

describe('Sumtime', function () {

  it('Constructor should return object with all required methods', function () {
    testSum = new SumTime(testStore);

    expect(testSum).to.be.an('object');
    expect(testSum).to.respondTo('resolutionKey');
    expect(testSum).to.respondTo('resolutionKeys');
    expect(testSum).to.respondTo('increment');
    expect(testSum).to.respondTo('get');
    expect(testSum).to.respondTo('getRange');
    expect(testSum).to.respondTo('buildDateRangeStrategy');
    expect(testSum).to.respondTo('getTotal');
    expect(testSum).to.respondTo('basicTotal');
  });

  describe('Data Point Processing', function () {
    it('Should respond gracefully to a lot of data points over a period of time', function (doneWithTest) {
      var toStore = 100000;
      var test = this;

      async.doWhilst(
        // Store a single randomized metric at a random datetime
        function singleIteration(doneWithOneIteration) {
          toStore--;
          testSum.increment(
            'testMetric',
            randomDate(new Date(2014, 11, 1), new Date(2015, 2, 14)),
            randomInt(2, 10),
            'minute',
            function () {
              doneWithOneIteration();
            }
          );
        },
        // Check to see if we've stored all of them yet.
        function testIfDone() {
          test.timeout(2000);
          if (toStore % 10000 === 0) {
            console.log(toStore + ' metrics left');
          }
          return toStore > 0;
        },
        // Done storing all
        doneWithTest
      );
    });
  });

  describe('Fetching ranges', function () {
    it('Should fetch a range at year resolution', function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 1, 1),
        new Date(2015, 6, 1),
        'year',
        function (err, range) {
          console.log(range);
          done();
        }
      );
    });

    it('Should fetch a range at month resolution', function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 1),
        new Date(2015, 3, 1),
        'month',
        function (err, range) {
          console.log(range);
          done();
        }
      );
    });

    it('Should fetch a range at week resolution', function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 1),
        new Date(2015, 3, 1),
        'week',
        function (err, range) {
          console.log(range);
          done();
        }
      );
    });

    it('Should fetch a range at day resolution', function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 1),
        new Date(2015, 3, 1),
        'day',
        function (err, range) {
          console.log(range);
          done();
        }
      );
    });
  });
});
