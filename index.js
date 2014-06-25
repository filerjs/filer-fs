var os = require('os');
var fs = require('fs');
var path = require('path');
var async = require('async');
var mkdirp = require('mkdirp');

function FSContext(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return path.join(prefix, key);
}

FSContext.prototype.put = function (key, value, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  // We do extra work to make sure typed buffer survive
  // being stored on disk and still get the right prototype later.
  if (Buffer.isBuffer(value)) {
    value = {
      __isBuffer: true,
      __array: value.toJSON()
    };
  }
  value = JSON.stringify(value);

  fs.writeFile(key, value, function(err) {
    if(err) {
      callback("Error: unable to write to disk. Error was " + err);
      return;
    }
    callback();
  });
};

FSContext.prototype.delete = function (key, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  fs.unlink(key, function(err) {
    if(err) {
      callback("Error: unable to delete key from disk. Error was " + err);
      return;
    }
    callback();
  });
};

FSContext.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }

  function removeEntries(pathname, callback) {
    fs.readdir(pathname, function(error, entries) {
      if(error) {
        callback(error);
        return;
      }

      entries = entries.map(function(filename) {
        return path.join(pathname, filename);
      });

      async.each(entries, function(filename, callback) {
        fs.unlink(filename, function(err) {
          if(err) {
            callback(err);
            return;
          }
        });
      },
      function(error) {
        if(error) {
          callback(error);
          return;
        }
      });
      callback(null);
    });
  }

  removeEntries(this.keyPrefix, callback);
};

FSContext.prototype.get = function (key, callback) {
  key = prefixKey(this.keyPrefix, key);
  fs.readFile(key, 'utf8', function(err, data) {
    if(err && err.code !== 'ENOENT') {
      callback("Error: unable to get key from disk. Error was " + err);
      return;
    }

    try {
      if(data) {
        data = JSON.parse(data);
        // Deal with special-cased flattened typed buffer (see put() below)
        if(data.__isBuffer) {
          data = new Buffer(data.__array);
        }
      }
      callback(null, data);
    } catch(e) {
      return callback(e);
    }
  });
};

function FSProvider(options) {
  this.name = options.name;
  this.root = path.normalize(options.root || path.join(os.tmpDir(), 'filer-data'));
  this.keyPrefix = options.keyPrefix;
}

FSProvider.isSupported = function() {
  return (typeof module !== 'undefined' && module.exports);
};

FSProvider.prototype.open = function(callback) {
  if(!this.keyPrefix) {
    callback("Error: Missing keyPrefix");
    return;
  }
  var that = this;
  var dir = path.join(this.root, this.keyPrefix);

  mkdirp(dir, function(err) {
    if (err && err.code != 'EEXIST') {
      callback(err);
      return;
    }
    that.keyPrefix = dir;

    fs.readdir(dir, function(err, entries) {
      if(err) {
        callback(err);
        return;
      }
      callback(null, entries.length === 0);
    });
  });
};

FSProvider.prototype.getReadOnlyContext = function() {
  return new FSContext({isReadOnly: true, keyPrefix: this.keyPrefix});
};

FSProvider.prototype.getReadWriteContext = function() {
  return new FSContext({isReadOnly: false, keyPrefix: this.keyPrefix});
};

module.exports = FSProvider;
