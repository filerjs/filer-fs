var os = require('os');
var fs = require('fs');
var path = require('path');
var async = require('async');
var mkdirp = require('mkdirp');

function u8toArray(u8) {
  var array = [];
  var len = u8.length;
  for(var i = 0; i < len; i++) {
    array[i] = u8[i];
  }
  return array;
}

function FSContext(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return path.join(prefix, key);
}

FSContext.prototype.put = function (key, value, callback) {console.log('put', key);
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  // We do extra work to make sure typed arrays survive
  // being stored on disk and still get the right prototype later.
  if (Object.prototype.toString.call(value) === "[object Uint8Array]") {
    value = {
      __isUint8Array: true,
      __array: u8toArray(value)
    };
  }
  value = JSON.stringify(value);

  fs.writeFile(key, value, function(err) {
    if(err) {
console.log('put failed', err);
      callback("Error: unable to write to disk. Error was " + err);
      return;
    }
    callback();
  });
};

FSContext.prototype.delete = function (key, callback) {console.log('delete', key);
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }
  key = prefixKey(this.keyPrefix, key);
  fs.unlink(key, function(err) {
    if(err) {
console.log('delete failed', err);
      callback("Error: unable to delete key from disk. Error was " + err);
      return;
    }
    callback();
  });
};

FSContext.prototype.clear = function (callback) {console.log('clear');
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }

  function removeEntries(pathname, callback) {
    fs.readdir(pathname, function(error, entries) {
      if(error) {
console.log('readdir failed', error);
        callback(error);
        return;
      }

      entries = entries.map(function(filename) {
        return path.join(pathname, filename);
      });

      async.each(entries, function(filename, callback) {
        console.error('unlink', filename);
        fs.unlink(filename, function(err) {
          if(err) console.log('unlink', filename, err);
          callback(err);
        });
      },
      function(error) {
        if(error) {
console.log('async each failed', error);
          callback(error);
          return;
        }
        callback();
      });
    });
  }

  removeEntries(this.keyPrefix, callback);
};

FSContext.prototype.get = function (key, callback) {console.log('get', key);
  key = prefixKey(this.keyPrefix, key);
  fs.readFile(key, 'utf8', function(err, data) {
    if(err && err.code !== 'ENOENT') {
console.log('get failed', err);
      callback("Error: unable to get key from disk. Error was " + err);
      return;
    }

    try {
      if(data) {
        data = JSON.parse(data);
        // Deal with special-cased flattened typed arrays (see put() below)
        if(data.__isUint8Array) {
          data = new Uint8Array(data.__array);
        }
      }
      callback(null, data);
    } catch(e) {
      callback(e);
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
