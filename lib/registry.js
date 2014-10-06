var EventEmitter = require('events').EventEmitter
  , ZooKeeper = require('zookeeper')
  , ZooKeeperError = require('./errors/zookeepererror')
  , NotFoundError = require('./errors/notfounderror')
  , LRU = require('lru-cache')
  , uuid = require('node-uuid').v4
  , shuffle = require('knuth-shuffle').knuthShuffle
  , util = require('util');


function Registry(options) {
  if (typeof options == 'string') {
    options = { prefix: options };
  }
  options = options || {};
  
  EventEmitter.call(this);
  this._prefix = options.prefix || '/srv';
  this._cache = new LRU(options.cache || 32);
}

/**
 * Inherit from `EventEmitter`.
 */
util.inherits(Registry, EventEmitter);

Registry.prototype.connect = function(options, readyListener) {
  if (readyListener) { this.once('ready', readyListener); }
  
  var self = this;
  var opts = {};
  opts.connect = options.connect;
  opts.timeout = 200000;
  opts.debug_level = ZooKeeper.ZOO_LOG_LEVEL_ERROR;
  //opts.debug_level = ZooKeeper.ZOO_LOG_LEVEL_DEBUG;
  opts.host_order_deterministic = false;
  this._client = new ZooKeeper(opts);
  this._client.on('close', this.emit.bind(this, 'close'));
  this._client.on('error', this.emit.bind(this, 'error'));
  
  this._client.connect(function(err) {
    if (err) { return; }
    self.emit('ready');
  });
}

Registry.prototype.close = function () {
  this._cache.reset();
  
  if (this._client) {
    this._client.close();
  }
}

Registry.prototype.announce = function(domain, type, data, cb) {
  if (typeof data == 'object') {
    data = JSON.stringify(data);
  }
  
  var client = this._client
    , uid = uuid()
    , dir = [ this._prefix, domain, encodeURIComponent(type) ].join('/')
    , path = [ dir, uid ].join('/');
    
  // http://zookeeper.apache.org/doc/r3.4.5/zookeeperProgrammers.html#ch_zkDataModel
    
  client.mkdirp(dir, function(err) {
    if (err) { return cb(err); }
    
    client.a_create(path, data, ZooKeeper.ZOO_EPHEMERAL, function(rc, error, path) {
      // TODO: Write test cases for these conditions:
      //  rc = -110  error = node exists
      if (rc != 0) {
        return cb(new ZooKeeperError(error, rc));
      }
      return cb();
    });
  });
  
  return uid;
}

Registry.prototype.unannounce = function(domain, type, uid, cb) {
  var client = this._client
    , path = [ this._prefix, domain, encodeURIComponent(type), uid ].join('/');
    
  client.a_delete_(path, -1, function(rc, error) {
    if (rc != 0) {
      return cb(new ZooKeeperError(error, rc));
    }
    return cb();
  });
}

Registry.prototype.domains = function(cb) {
  this._ls(this._prefix, function (err, res) {
    if (err) {
      return cb(err);
    }
    return cb(null, res);
  });
}

Registry.prototype.resolve = function(domain, type, cb) {
  var self = this
    , client = this._client
    , dir = [ this._prefix, domain, encodeURIComponent(type) ].join('/')
    , cached = this._cache.get(dir);
  
  if (cached) {
    process.nextTick(function() {
      if (cached.length == 0) { return cb(new NotFoundError('No records for "' + type + '"')); }
      // randomize the array as a pseudo-load balancing technique
      cb(null, shuffle(cached.slice(0)));
    });
    return;
  }
  
  client.mkdirp(dir, function(err) {
    if (err) { return cb(err); }
  
    client.aw_get_children(dir, self.__watchcb.bind(self), function(rc, error, children) {
      if (rc != 0) {
        switch (rc) {
        case ZooKeeper.ZNONODE:
          return cb(new NotFoundError(error));
        default:
          return cb(new ZooKeeperError(error, rc));
        }
      }
    
      var records = []
        , idx = 0;
    
      function iter(err) {
        if (err) { return cb(err); }
     
        var child = children[idx++];
        if (!child) {
          self._cache.set(dir, records);
          if (records.length == 0) { return cb(new NotFoundError('No records for "' + type + '"')); }
          // randomize the array as a pseudo-load balancing technique
          return cb(null, shuffle(records.slice(0)));
        }
      
        var path = [ dir, child ].join('/');
        client.a_get(path, null, function(rc, error, stat, data) {
          if (rc != 0) {
            if (rc === ZooKeeper.ZNONODE) {
              // This situation arises when an ephemeral node is purged between the time it
              // was listed as a child and when its data was gotten.  We simply skip over
              // these occurences, continuing to get nodes that still exist.
              return iter();
            }
            return iter(new ZooKeeperError(error, rc));
          }
        
          var str = data.toString()
            , json;
          try {
            json = JSON.parse(data);
            records.push(json);
          } catch (_) {
            records.push(str);
          }
          iter();
        });
      }
      iter();
    });
  });
}

Registry.prototype.services = 
Registry.prototype.types = function(domain, cb) {
  var dir = [ this._prefix, domain ].join('/');
  this._ls(dir, function (err, res) {
    if (err) {
      return cb(err);
    }
    var types = res.map(function(c) { return decodeURIComponent(c); });
    return cb(null, types);
  });
}

Registry.prototype._ls = function(path, cb) {
  var client = this._client;
  
  client.a_get_children(path, null, function(rc, error, children) {
    if (rc != 0) {
      switch (rc) {
      case ZooKeeper.ZNONODE:
        return cb(new NotFoundError(error));
      default:
        return cb(new ZooKeeperError(error, rc));
      }
    }
    
    return cb(null, children);
  });
}

Registry.prototype.__watchcb = function(event, state, dir) {
  var self = this
    , client = this._client
    , segs = dir.split('/')
    , type = decodeURIComponent(segs[segs.length - 1])
    , domain = segs[segs.length - 2]
    , cached = this._cache.has(dir);
  
  if (!cached) { return; }
  
  switch (event) {
  case ZooKeeper.ZOO_CHILD_EVENT:
    client.aw_get_children(dir, this.__watchcb.bind(this), function(rc, error, children) {
      if (rc != 0) {
        // unexpected error, invalidate cached records for this service
        self._cache.del(dir);
        return;
      }
      
      var records = []
        , idx = 0;
    
      function iter(err) {
        if (err) {
          // unexpected error, invalidate cached records for this service
          self._cache.del(dir);
          return;
        }
     
        var child = children[idx++];
        if (!child) {
          self._cache.set(dir, records);
          self.emit('services', domain, type, records);
        }
      
        var path = [ dir, child ].join('/');
        client.a_get(path, null, function(rc, error, stat, data) {
          if (rc != 0) {
            if (rc === ZooKeeper.ZNONODE) {
              // This situation arises when an ephemeral node is purged between the time it
              // was listed as a child and when its data was gotten.  We simply skip over
              // these occurences, continuing to get nodes that still exist.
              return iter();
            }
            return iter(new ZooKeeperError(error, rc));
          }
        
          var str = data.toString()
            , json;
          try {
            json = JSON.parse(data);
            records.push(json);
          } catch (_) {
            records.push(str);
          }
          iter();
        });
      }
      iter();
    });
    break;
  }
  
}


module.exports = Registry;
