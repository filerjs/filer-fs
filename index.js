var os = require('os');
var fs = require('fs');
var path = require('path');
var mkdirp = require('mkdirp');
var rimraf = require('rimraf');

function FSContext(options) {
  this.readOnly = options.isReadOnly;
  this.keyPrefix = options.keyPrefix;
}

function prefixKey(prefix, key) {
  return path.join(prefix, key);
}

function _put(keyPrefix, key, value, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }

  var keyPath = prefixKey(keyPrefix, key);
  fs.writeFile(keyPath, value, function(err) {
    if(err) {
      return callback("Error: unable to write to disk. Error was: " + err.stack);
    }

    callback();
  });
}
FSContext.prototype.putObject = function(key, value, callback) {
  var json = JSON.stringify(value);
  _put(this.keyPrefix, key, json, callback);
};
FSContext.prototype.putBuffer = function(key, value, callback) {
  _put(this.keyPrefix, key, value, callback);
};

FSContext.prototype.delete = function (key, callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }

  var keyPath = prefixKey(this.keyPrefix, key);
  fs.unlink(keyPath, function(err) {
    if(err && err.code !== 'ENOENT') {
      return callback("Error: unable to delete key from disk. Error was " + err);
    }
    callback();
  });
};

FSContext.prototype.clear = function (callback) {
  if(this.readOnly) {
    return callback("Error: Write operation on readOnly context.");
  }

  var dir = this.keyPrefix;

  // rm -fr <user/fs/dir/root>
  rimraf(dir, function(err) {
    if(err) {
      return callback(err);
    }

    // Now create it again so we have an empty root for this user's fs
    mkdirp(dir, callback);
  });
};

function _get(keyPrefix, encoding, key, callback) {
  var keyPath = prefixKey(keyPrefix, key);
  fs.readFile(keyPath, {encoding: encoding}, function(err, data) {
    if(err && err.code !== 'ENOENT') {
      return callback("Error: unable to get key from disk. Error was " + err);
    }

    callback(null, data);
  });
}
FSContext.prototype.getObject = function(key, callback) {
  _get(this.keyPrefix, 'utf8', key, function(err, data) {
    if(err) {
      return callback(err);
    }

    if(data) {
      try {
        data = JSON.parse(data);
      } catch(e) {
        return callback(e);
      }
    }

    callback(null, data);
  });
};
FSContext.prototype.getBuffer = function(key, callback) {
  _get(this.keyPrefix, /* leave as raw Buffer */ null, key, callback);
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
    return callback("Error: Missing keyPrefix");
  }

  var that = this;
  var dir = path.join(this.root, this.keyPrefix);

  mkdirp(dir, function(err) {
    if (err && err.code !== 'EEXIST') {
      return callback(err);
    }

    that.keyPrefix = dir;

    // Check for any nodes. We need a least a supernode
    // or this will need to be formatted.
    fs.readdir(dir, function(err, entries) {
      if(err) {
        return callback(err);
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
