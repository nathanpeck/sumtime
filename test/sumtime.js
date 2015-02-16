var InMemory = require(process.cwd() + '/lib/datastores/memory.js'),
    SumTime  = require(process.cwd() + '/lib/sumtime.js'),
    async    = require('async'),
    _        = require('lodash'),
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
            randomDate(
              // Remember that JS masochists decided that months should be zero based,
              // but not days. *facepalm.jpg*
              new Date(2014, 11, 1),  // Dec 1, 2014
              new Date(2015, 2, 14)), // March 14, 2015
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

  describe('Fetching range at year resolution', function () {
    var yearRange;

    before(function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2013, 1, 1), // Feb 1, 2013
        new Date(2016, 6, 1), // July 1, 2016
        'year',
        function (err, range) {
          yearRange = range;
          done();
        }
      );
    });

    it('Range should have 4 keys', function () {
      expect(_.keys(yearRange)).to.have.length(4);
    });

    it('Values should match up with expected years', function () {
      var values = _.values(yearRange);
      expect(values[0]).to.equal(0);
      expect(values[1]).to.be.above(0);
      expect(values[2]).to.be.above(0);
      expect(values[3]).to.equal(0);
    });
  });

  describe('Fetching range at month resolution', function () {
    var monthRange;

    before(function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 1), // Nov 1, 2014
        new Date(2015, 3, 1), // Apr 1, 2015
        'month',
        function (err, range) {
          monthRange = range;
          done();
        }
      );
    });

    it('Range should have 6 keys', function () {
      expect(_.keys(monthRange)).to.have.length(6);
    });

    it('Keys should match up with expected months', function () {
      var values = _.values(monthRange);
      expect(values[0]).to.equal(0);
      expect(values[1]).to.be.above(0);
      expect(values[2]).to.be.above(0);
      expect(values[3]).to.be.above(0);
      expect(values[4]).to.be.above(0);
      expect(values[5]).to.equal(0);
    });
  });

  describe('Fetching range at week resolution', function () {
    var weekRange;

    before(function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 25), // Nov 25, 2014
        new Date(2015, 2, 20), // March 20, 2015
        'week',
        function (err, range) {
          weekRange = range;
          done();
        }
      );
    });

    it('Range should have 17 keys', function () {
      expect(_.keys(weekRange)).to.have.length(17);
    });

    it('Keys should match up with expected weeks', function () {
      var values = _.values(weekRange);
      expect(values[0]).to.equal(0);
      expect(values[16]).to.equal(0);

      for (var i = 1; i < 16; i++) {
        expect(values[i]).to.be.above(0);
      }
    });
  });

  describe('Fetching range at day resolution', function () {
    var dayRange;

    before(function (done) {
      testSum.getRange(
        'testMetric',
        new Date(2014, 10, 25), // Nov 25, 2014
        new Date(2015, 2, 20), // March 20, 2015
        'day',
        function (err, range) {
          dayRange = range;
          done();
        }
      );
    });

    it('Range should have 116 keys', function () {
      expect(_.keys(dayRange)).to.have.length(116);
    });
  });
});
