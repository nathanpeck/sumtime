var InMemory = require(process.cwd() + '/lib/datastores/memory.js'),
    expect     = require('chai').expect;

describe('In Memory Datastore', function () {

  it('Constructor should return object with all required methods', function () {
    var testStore = new InMemory();

    expect(testStore).to.be.an('object');
    expect(testStore).to.respondTo('hincrby');
    expect(testStore).to.respondTo('hget');
    expect(testStore).to.respondTo('hmget');

    testStore = null; // Destroy
  });

  var sharedStore;

  it('Setting first field in first hash', function (done) {
    sharedStore = new InMemory();

    sharedStore.hincrby('first', 'fieldOne', 5, function () {
      done();
    });
  });

  it('Fetching first field in first hash', function (done) {
    sharedStore.hget('first', 'fieldOne', function (err, value) {
      expect(value).to.equal(5);
      done();
    });
  });

  it('Incrementing first field in first hash', function (done) {
    sharedStore.hincrby('first', 'fieldOne', 5, function () {
      done();
    });
  });

  it('Fetching first field in first hash', function (done) {
    sharedStore.hget('first', 'fieldOne', function (err, value) {
      expect(value).to.equal(10);
      done();
    });
  });

  it('Setting second field in first hash', function (done) {
    sharedStore.hincrby('first', 'fieldTwo', 3, function () {
      done();
    });
  });

  it('Fetching second field in first hash', function (done) {
    sharedStore.hget('first', 'fieldTwo', function (err, value) {
      expect(value).to.equal(3);
      done();
    });
  });

  it('Incrementing second field in first hash', function (done) {
    sharedStore.hincrby('first', 'fieldTwo', 3, function () {
      done();
    });
  });

  it('Fetching second field in first hash', function (done) {
    sharedStore.hget('first', 'fieldTwo', function (err, value) {
      expect(value).to.equal(6);
      done();
    });
  });

  it('Fetching nonexistant field in first hash', function (done) {
    sharedStore.hget('first', 'fieldThree', function (err, value) {
      expect(value).to.equal(null);
      done();
    });
  });

  it('Setting first field in second hash', function (done) {
    sharedStore.hincrby('second', 'fieldOne', 4, function () {
      done();
    });
  });

  it('Fetching first field in second hash', function (done) {
    sharedStore.hget('second', 'fieldOne', function (err, value) {
      expect(value).to.equal(4);
      done();
    });
  });

  it('Fetching nonexistant hash should return null', function (done) {
    sharedStore.hget('foo', 'bar', function (err, value) {
      expect(value).to.equal(null);
      done();
    });
  });

  it('Fetching multiple fields from first hash should return array', function (done) {
    sharedStore.hmget('first', ['fieldOne', 'fieldTwo'], function (err, value) {
      expect(value).to.an('array').with.length(2);
      expect(value[0]).to.equal(10);
      expect(value[1]).to.equal(6);
      done();
    });
  });

  it('Fetching multiple fields from first hash along with nonexistant field should return array', function (done) {
    sharedStore.hmget('first', ['fieldOne', 'fieldTwo', 'fieldThree'], function (err, value) {
      expect(value).to.an('array').with.length(3);
      expect(value[0]).to.equal(10);
      expect(value[1]).to.equal(6);
      expect(value[2]).to.equal(null);
      done();
    });
  });
});
