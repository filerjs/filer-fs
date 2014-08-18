var expect = require('expect.js'),
    FSProvider = require(".."),
    randomName,
    randomKeyPrefix;

function guid () {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
    var r = Math.random()*16|0, v = c === 'x' ? r : (r&0x3|0x8);
    return v.toString(16);
  }).toUpperCase();
}

describe("Filer.FileSystem.providers.FS", function() {
  it("is supported -- if it isn't, none of these tests can run.", function() {
    expect(FSProvider.isSupported).to.be.true;
  });

  it("has open, getReadOnlyContext, and getReadWriteContext instance methods", function() {
    var FS = new FSProvider({name: guid(), keyPrefix: guid()});
    expect(FS.open).to.be.a('function');
    expect(FS.getReadOnlyContext).to.be.a('function');
    expect(FS.getReadWriteContext).to.be.a('function');
  });

  describe("open an FS provider", function() {
    var _provider;

    beforeEach(function() {
      randomName = guid();
      randomKeyPrefix = guid();
      _provider = new FSProvider({name: randomName, keyPrefix: randomKeyPrefix });
    });

    it("should open a new FS", function(done) {
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        expect(error).not.to.exist;
        expect(firstAccess).to.be.true;
        done();
      });
    });
  });

  describe("Read/Write operations on an FS provider", function() {
    var _provider;

    beforeEach(function() {
      _provider = new FSProvider({name: randomName, keyPrefix: randomKeyPrefix });
    });

    afterEach(function(done){
      provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.clear(function(error) {
        if (error) {
          throw error;
        }
        expect(error).not.to.exist;
        done();
        });
      });
    });

    it("should allow put() and get()", function(done) {
      var data = new Buffer([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if(error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.putBuffer("key", data, function(error) {
          if(error) {
            throw error;
          }
          context.getBuffer("key", function(error, result) {
            expect(error).not.to.exist;
            expect(result).to.exist;
            expect(result).to.eql(data);
            done();
          });
        });
      });
    });

    it("should allow delete()", function(done) {
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.putObject("key", "value", function(error) {
          if (error) {
            throw error;
          }
          context.delete("key", function(error) {
            if (error) {
              throw error;
            }
            context.getObject("key", function(error, result) {
              expect(error).not.to.exist;
              expect(result).not.to.exist;
              done();
            });
          });
        });
      });
    });

    it("should allow clear()", function(done) {
      var data1 = new Buffer([5, 2, 5]);
      var data2 = new Buffer([10, 20, 50]);

      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadWriteContext();
        context.putBuffer("key1", data1, function(error) {
          if (error) {
            throw error;
          }
          expect(error).not.to.exist;
          context.putBuffer("key2", data2, function(error) {
            if (error) {
              throw error;
            }
            expect(error).not.to.exist;
            context.clear(function(error) {
              if (error) {
                throw error;
              }
              context.getBuffer("key1", function(error, result) {
               expect(error).to.exist;
                expect(result).not.to.exist;

                context.getBuffer("key2", function(error, result) {
                  expect(error).to.exist;
                  expect(result).not.to.exist;
                  done();
                });
              });
            });
          });
        });
      });
    });

    it("should fail when trying to write on ReadOnlyContext", function(done) {
      var data1 = new Buffer([5, 2, 5]);
      var provider = _provider;
      provider.open(function(error, firstAccess) {
        if (error) {
          throw error;
        }
        expect(firstAccess).to.be.true;
        var context = provider.getReadOnlyContext();
        context.putBuffer("key1", data1, function(error) {
          expect(error).to.exist;
          done();
        });
      });
    });
  });

});
