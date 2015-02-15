// Simple in memory datastore for Sumtime
// This should NOT be used in a production environment unless you are a glutton
// for punishment.
//
// The primary use case for this datastore is for unit testing purposes, if you
// don't want to have a Redis or other proper datastore in your test environment.

var _ = require('lodash');

/**
 * Construct an InMemory datastre
 * @constructor
 */
function InMemory() {
  this.hashes = {};
}

/**
 * Increment a particular field in a particular hash by a designated value.
 * Behaves the same as Redis HINCRBY command
 * @method
 * @param {string} hash - The key of the hash object itself
 * @param {string} field - A specific field to increment on the hash object
 * @param {integer} value - How much to increment the field by
 * @param {function} callback - Callback after done incrementing
 */
InMemory.prototype.hincrby = function (hash, field, value, callback) {
  if (!this.hashes[hash]) {
    this.hashes[hash] = {};
  }

  if (!this.hashes[hash][field]) {
    this.hashes[hash][field] = value;
  }
  else {
    this.hashes[hash][field] += value;
  }

  callback();
};

/**
 * Fetch the value in a specific field of a specific hash
 * Behaves the same as Redis HGET command
 * @method
 * @param {string} hash - The key of the hash object itself
 * @param {string} field - A specific field to fetch
 * @param {function} callback - Callback to return value to
 */
InMemory.prototype.hget = function (hash, field, value, callback) {
  // Make sure hash object exists.
  if (!this.hashes[hash]) {
    callback(null, null);
  }

  if (!this.hashes[hash][field]) {
    // Hash exists but field doesn't
    callback(null, null);
  }
  else {
    callback(this.hashes[hash][field]);
  }
};

/**
 * Fetch multiple field values from a specific hash
 * Behaves the same as Redis HMGET command
 * @method
 * @param {string} hash - The key of the hash object itself
 * @param {string} field - An array of fields to fetch
 * @param {function} callback - Callback to return values to
 */
InMemory.prototype.hmget = function (hash, fields, value, callback) {
  var self = this;

  var response = _.map(fields, function (field) {
    // Make sure hash object exists.
    if (!self.hashes[hash]) {
      return null;
    }

    if (!self.hashes[hash][field]) {
      // Hash exists but field doesn't
      return null;
    }
    else {
      return self.hashes[hash][field];
    }
  });

  callback(null, response);
};

module.exports = InMemory;
