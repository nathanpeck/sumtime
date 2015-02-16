// General purpose wrapper for storing time based values in an underlying
// key value data store, using period based keys to allow retrieval of
// values from specific times, value ranges at a specific resolution,
// and/or sums of data points over a specific time period.
var moment = require('moment'),
    async  = require('async'),
    _      = require('lodash');

/**
 * Construct a Sumtime
 * @constructor
 * @param {datastore} datastore - A DataStore instance for storing the key value data.
 * @param {object} [options] - Optional configurations
 *    @property {integer} [fetchLimit] - Optional read limit, for protecting against ridiculously high resolution
 *                                       lookups over ranges.
 */
function Sumtime(datastore, options) {
  this.datastore = datastore;

  // If user didn't specify any options.
  if (!_.isObject(options)) {
    options = {};
  }

  // Defaults for all options
  this.options = _.extend(
    {
      /**
       * If the number of keys that would be fetched by a getRange or getTotal operation
       * is higher than this threshold return an error instead . This protects against abusive =
       * operations such as getRange for the last twenty years at second resolution. If this is set
       * to zero then the protection is diabled.
       * @constant
       */
      fetchLimit: 0
    },
    options
  );
}

/**
 * List of resolutions that data can be stored at.
 * @constant
 */
Sumtime.prototype.resolutions = {
  'second': null,
  'minute': null,
  'hour': null,
  'day': null,
  'week': null,
  'month': null,
  'year': null,
};

/**
 * Given a timestamp and a resolution return the datastore key at the
 * specified resolution that would store data for the given timestamp.
 * @method
 * @param {Moment} time - The Moment timestamp to fetch a datastore key for
 * @param {string} period - The resolution of the desired datastore key
 */
Sumtime.prototype.resolutionKey = function (time, resolution) {
  if (resolution == 'year')
    return time.year();
  else if (resolution == 'month')
    return time.year() + '-' + time.month();
  else if (resolution == 'week')
    return time.year() + '-w-' + time.week();
  else if (resolution == 'day')
    return time.year() + '-' + time.month() + '-' + time.date();
  else if (resolution == 'hour')
    return time.year() + '-' + time.month() + '-' + time.date() + '-' + time.hour();
  else if (resolution == 'minute')
    return time.year() + '-' + time.month() + '-' + time.date() + '-' + time.hour() + '-' + time.minute();
  else if (resolution == 'second')
    return time.year() + '-' + time.month() + '-' + time.date() + '-' + time.hour() + '-' + time.minute() + '-' + time.second();
};

/**
 * Given a timestamp and a list of resolutions return all the keys that
 * must be used to store data for the specified time at the specified resolutions.
 * @method
 * @param {Moment} time - The Moment timestamp to fetch a datastore key for
 * @param {array} lowestResolution - The lowest resolution to include
 */
Sumtime.prototype.resolutionKeys = function (time, lowestResolution) {
  var resolutions = [];

  var year = time.year();
  resolutions.push(['year', year]);

  if (lowestResolution == 'year') {
    return resolutions;
  }

  var month = time.month();
  resolutions.push(['month', year + '-' + month]);

  if (lowestResolution == 'month') {
    return resolutions;
  }

  var week = time.week();
  resolutions.push(['week', year + '-w-' + week]);

  if (lowestResolution == 'week') {
    return resolutions;
  }

  var day = time.date();
  resolutions.push(['day', year + '-' + month + '-' + day]);

  if (lowestResolution == 'day') {
    return resolutions;
  }

  var hour = time.hour();
  resolutions.push(['hour', year + '-' + month + '-' + day + '-' + hour]);

  if (lowestResolution == 'hour') {
    return resolutions;
  }

  var minute = time.minute();
  resolutions.push(['minute', year + '-' + month + '-' + day + '-' + hour + '-' + minute]);

  if (lowestResolution == 'minute') {
    return resolutions;
  }

  var second = time.second();
  resolutions.push(['second', year + '-' + month + '-' + day + '-' + hour + '-' + minute + '-' + second]);

  return resolutions;
};

/**
 * Increment the statistics for a given statistic key on all resolutions
 * @method
 * @param {string} key - The name of the key to store in the database
 * @param {Date} time - The JavaScript Date of the data we are storing.
 * @param {integer} value - The value of the key
 * @param {string} resolution - The lowest resolution at which to store the data. Useful
 *                              if you want to avoid storing data at lower resolutions like minute or second.
 * @param {function} callback - Callback after done incrementing
 */
Sumtime.prototype.increment = function (key, time, value, resolution, callback) {
  var parsedTime = moment(time);
  var self = this;

  // Trim the resolution of the statistics down to only what
  // we want to gather for this statistic.
  if (!self.resolutions[resolution]) {
    resolution = 'minute'; // Default
  }

  async.each(
    // List of period counters to increment.
    self.resolutionKeys(parsedTime, resolution),
    // Increment a single time period's counter
    function (resKey, done) {
      self.datastore.hincrby(
        key,
        resKey[1],
        value,
        done
      );
    },
    // Done incrementing all the period keys.
    callback
  );
};

/**
 * Fetch totals for a data key at a specific time, at the given resolutions
 * @method
 * @param {string} key - The name of the key to store in the database
 * @param {Date} time - The JavaScript Date of the data we are storing.
 * @param {array} resolutions - List of resolutions to fetch total for
 * @param {function} callback - Callback to return result too
 */
Sumtime.prototype.get = function (key, time, resolutions, callback) {
  var parsedTime = moment(time);
  var data = {}, self = this;

  // If the user just passed one single resolution then
  // turn that into an array of one item.
  if (typeof resolutions == 'string') {
    resolutions = [resolutions];
  }

  async.each(
    // List of period keys to get.
    self.resolutionKeys(parsedTime, resolutions),
    // Get a single value.
    function (periodKey, doneGettingPeriodValue) {
      self.datastore.hget(
        key,
        periodKey[1],
        function (err, value) {
          data[periodKey[0]] = value;
          doneGettingPeriodValue();
        }
      );
    },
    // Return all the values we fetched.
    function () {
      callback(null, data);
    }
  );
};

/**
 * Fetch the range of keys between startTimestamp and endTimestamp
 * at the given resolution.
 * @method
 * @param {string} key - The name of the key to store in the database
 * @param {Date} startTimestamp - The start of the date range to fetch
 * @param {Date} endTimestamp - The end of the date range to fetch
 * @param {array} resolutions - The resolution to fetch data at
 * @param {function} callback - Callback to return results too
 */
Sumtime.prototype.getRange = function (key, startTimestamp, endTimestamp, resolution, callback) {
  var startMoment = moment(startTimestamp);
  var endMoment = moment(endTimestamp);

  // Normalize the timestamps in case user passes them in reversed order.
  if (endMoment.isBefore(startMoment)) {
    var temp = startMoment;
    startMoment = endMoment;
    endMoment = temp;
  }

  // Default in case some fucked up resolution string is passed.
  if (!_.isString(resolution)) {
    resolution = 'day';
  }

  startMoment = startMoment.startOf(resolution);
  endMoment = endMoment.endOf(resolution);

  var timeKeysToGet = [], moments = [];
  var iterations = endMoment.diff(startMoment, resolution);

  // Check for a lookup that goes over the threshold
  if (this.options.fetchLimit > 0) {
    if (iterations > this.options.fetchLimit) {
      return callback('Operation would require ' + iterations + ' to complete ' +
        'which is over the lookup limit of ' + this.options.fetchLimit);
    }
  }

  // Build a list of keys to look up to cover the date range at the desired resolution.
  for (var i = 0; i <= iterations; i++) {
    moments.push(startMoment.format());
    timeKeysToGet.push(this.resolutionKey(startMoment, resolution));
    startMoment.add(1, resolution);
  }

  if (timeKeysToGet.length == 1) {
    // Optimize for fetching one key at a time.
    this.datastore.hget(
      key,
      timeKeysToGet[0],
      function (err, result) {
        if (err)
          callback(err);
        else
        {
          var dictionary = {};
          dictionary[moments[0]] = result | 0;
          callback(null, dictionary);
        }
      }
    );
  }
  else {
    // Optimize for fetching multiple keys at once.
    this.datastore.hmget(
      key,
      timeKeysToGet,
      function (err, history) {
        if (err)
          callback(err);
        else
        {
          history = _.map(history, function (value) {
            if (value === null)
              return 0;
            else
              return parseInt(value, 10);
          });
          callback(null, _.zipObject(moments, history));
        }
      }
    );
  }
};

/**
 * Build an optimal strategy for looking up the total for between a start and end timestamp
 * at a min resolution.
 * @method
 * @param {Date} startTimestamp - The beginning of the strategy
 * @param {Date} endTimestamp - The end of the stragey
 * @param {array} resolutions - The lowest resolution to match towards the start and end times
 * @param {function} callback - Callback to return results too
 */
Sumtime.prototype.buildDateRangeStrategy = function (startTimestamp, endTimestamp, minResolution, verbose) {
  var startMoment = moment(startTimestamp);
  var endMoment = moment(endTimestamp);

  if (endMoment.isBefore(startMoment)) {
    // User passed in the timestamps in reverse order.
    var temp = startMoment;
    startMoment = endMoment;
    endMoment = temp;
  }
  else if (startMoment.isSame(endMoment, minResolution)) {
    // User passed in two timestamps that are the same
    // and therefore only require one lookup
    return [{
      type: minResolution,
      date: startMoment.format('YYYY-MM-DD'),
      moment: startMoment
    }];
  }

  // Trim and optimize the timestamps.
  startMoment.startOf(minResolution);
  endMoment.startOf(minResolution);

  if (verbose)
    console.log('Finding keys for range from ' + startMoment.format('YYYY-MM-DD') + ' to ' + endMoment.format('YYYY-MM-DD'));

  // Trim the resolution of the statistics down to only what
  // we want to gather for this statistic.
  var resolutionsToCheck;
  var index = this.resolutions.indexOf(minResolution);
  if (index != -1) {
    resolutionsToCheck = this.resolutions.slice(0, index + 1);
  }
  else {
    resolutionsToCheck = this.resolutions;
  }

  var resolutionCapacities = [];

  // Look at the time period to see how many periods of each resolution could fit into
  // this time period, using arbitrary start and end dates for each sub period.
  _.each(resolutionsToCheck, function (resolutionString) {
    var difference = endMoment.diff(startMoment, resolutionString);
    if (difference > 0) {
      resolutionCapacities.push({
        type: resolutionString,
        capacity: difference
      });
    }
  });

  if (verbose) {
    console.log(
      'The date range has the following capacities at different resolutions: ' +
      JSON.stringify(resolutionCapacities, null, 2)
    );
  }

  // Now finetune our selections to find the largest time period
  // where a key or keys fit entirely within the date range.
  var currentKey = moment(startMoment).startOf(minResolution);
  var endOfCurrentKey;
  var chosenResolution = _.find(resolutionCapacities, function (resolution) {
    currentKey = moment(startMoment).startOf(resolution.type);
    if (currentKey.isBefore(startMoment))
      currentKey.add(1, resolution.type);

    endOfCurrentKey = moment(currentKey).endOf(resolution.type);
    return endOfCurrentKey.isBefore(endMoment);
  });

  if (verbose) {
    console.log(
      'Selected the following resolution as the largest resolution where a key will fit fully within the range: ' +
      JSON.stringify(chosenResolution, null, 2)
    );
  }

  // Now that we found the largest first key that fits entirely in the date range we need
  // to see if we continue filling the date range with more subsequent keys if needed
  var firstKey = currentKey.clone();
  var selectedKeys = [];
  while (endOfCurrentKey.isBefore(endMoment) | endOfCurrentKey.isSame(endMoment, minResolution)) {
    selectedKeys.push({
      type: chosenResolution.type,
      date: currentKey.format(),
      moment: currentKey.clone()
    });
    currentKey.add(1, chosenResolution.type);
    endOfCurrentKey = moment(currentKey).endOf(chosenResolution.type);
  }

  if (verbose) {
    console.log(
      'Found the following list of keys that all fit fully within the range: ' +
      JSON.stringify(selectedKeys, null, 2)
    );
  }

  // Now we need to repeat the process to fill up and space left over at the beginning and end if needed.
  var frontKeys = [], backKeys = [];
  if (firstKey.isAfter(startMoment)) {
    firstKey.subtract(1, minResolution);
    if (verbose)
      console.log('Still need to find keys to cover the start of date range');
    frontKeys = this.buildDateRangeStrategy(startMoment, firstKey, minResolution, verbose);
  }

  if (currentKey.isBefore(endMoment)) {
    if (verbose)
      console.log('Still need to find keys to cover the end of date range');
    backKeys = this.buildDateRangeStrategy(currentKey, endMoment, minResolution, verbose);
  }

  selectedKeys = frontKeys.concat(selectedKeys);
  selectedKeys = selectedKeys.concat(backKeys);

  if (verbose) {
    console.log(
      'Key range found:' +
      JSON.stringify(selectedKeys, null, 2)
    );
  }

  return selectedKeys;
};

/**
 * Fetch the total of a specific key between a start and end timestamp
 * @method
 * @param {Date} key - The key to fetch
 * @param {Date} startTimestamp - The beginning of the strategy
 * @param {Date} endTimestamp - The end of the stragey
 * @param {array} minResolution - The lowest resolution to match towards the start and end times
 * @param {function} callback - Callback to return the total to
 */
Sumtime.prototype.getTotal = function (key, startTimestamp, endTimestamp, minResolution, callback) {
  var self = this;

  // Build an optimal strategy for looking up the total between the given
  // start and endtime.
  var strategy = self.buildDateRangeStrategy(startTimestamp, endTimestamp, minResolution);

  // Fetch the keys from Redis.
  this.datastore.hmget(
    key,
    // Translate all the moments in the strategy into keys to fetch
    _.map(strategy, function (singleKeyDetails) {
      return self.periodKey(singleKeyDetails.moment, singleKeyDetails.type);
    }),
    function (err, values) {
      // Add up the total and return it.
      callback(
        null,
        _.reduce(
          values,
          function sum(memo, num) {
            if (num === null)
              return memo;
            else
              return memo + parseInt(num, 10);
          },
          0
        )
      );
    }
  );
};

/**
 * Fetch the total of a specific key between a start and end timestamp using a basic,
 * unoptimized strategy. This does significantly more datastore lookups than
 * the optimized strategy.
 * @method
 * @param {Date} key - The key to fetch
 * @param {Date} startTimestamp - The beginning of the strategy
 * @param {Date} endTimestamp - The end of the stragey
 * @param {array} minResolution - The lowest resolution to match towards the start and end times
 * @param {function} callback - Callback to return the total to
 */
Sumtime.prototype.basicTotal = function (key, startTimestamp, endTimestamp, minResolution, callback) {
  var self = this;
  async.waterfall(
    [
      //Get the data at a day resolution
      function fetchDayStats(doneGettingStats) {
        self.getRange(key, startTimestamp, endTimestamp, minResolution, doneGettingStats);
      },

      //Total up the stats over the range.
      function totalStats(stats, doneTotalling) {
        doneTotalling(
          null,
          _.reduce(
            _.values(stats),
            function sum(memo, num) {
              return memo + num;
            },
            0
          )
        );
      }
    ],
    callback
  );
};

module.exports = Sumtime;
