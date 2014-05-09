var EventEmitter = require('events').EventEmitter
  , ZooKeeper = require('zookeeper')
  , ZooKeeperError = require('./errors/zookeepererror')
  , uuid = require('node-uuid').v4
  , util = require('util');


function Registry(prefix) {
  EventEmitter.call(this);
  this._prefix = prefix || '/srv';
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
  opts.debug_level = ZooKeeper.ZOO_LOG_LEVEL_WARN;
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
}

Registry.prototype.resolve = function(domain, type, cb) {
  var client = this._client
    , dir = [ this._prefix, domain, encodeURIComponent(type) ].join('/');
  
  // TODO: This could be optimized by setting watches and keeping a (tunable)
  //       LRU cache of recently resolved services
  
  client.a_get_children(dir, null, function(rc, error, children) {
    if (rc != 0) {
      return cb(new ZooKeeperError(error, rc));
    }
    
    // TODO: Randomize children, as a pseudo-load balancing strategy, since there
    //       is no priority here
    
    var records = []
      , idx = 0;
    
    function iter(err) {
      if (err) { return cb(err); }
     
      var child = children[idx++];
      if (!child) { return cb(null, records); }
      
      var path = [ dir, child ].join('/');
      client.a_get(path, null, function(rc, error, stat, data) {
        if (rc != 0) {
          return iter(new ZooKeeperError(error, rc));
        }
        
        // TODO: if ephemeral node goes away, don't treat as error, simply don't add record
        
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
}

Registry.prototype.services = function(domain, cb) {
  var client = this._client
    , dir = [ this._prefix, domain ].join('/');
  
  client.a_get_children(dir, null, function(rc, error, children) {
    if (rc != 0) {
      return cb(new ZooKeeperError(error, rc));
    }
    
    var types = children.map(function(c) { return decodeURIComponent(c); });
    return cb(null, types);
  });
}


module.exports = Registry;
