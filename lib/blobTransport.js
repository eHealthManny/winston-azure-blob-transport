(function() {
  var BlobTransport, HOUR_IN_MILLISECONDS, MAX_BLOCK_SIZE, MB, Promise, Transport, _, async, azure, chunk, debug, errorToJson, util, winston,
    bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; },
    extend = function(child, parent) { for (var key in parent) { if (hasProp.call(parent, key)) child[key] = parent[key]; } function ctor() { this.constructor = child; } ctor.prototype = parent.prototype; child.prototype = new ctor(); child.__super__ = parent.prototype; return child; },
    hasProp = {}.hasOwnProperty;

  debug = require("debug")("winston-blob-transport");

  _ = require("lodash");

  util = require("util");

  errorToJson = require("error-to-json");

  azure = require("azure-storage");

  async = require("async");

  winston = require("winston");

  chunk = require("chunk");

  Promise = require("bluebird");

  Transport = winston.Transport;

  MAX_BLOCK_SIZE = azure.Constants.BlobConstants.MAX_BLOCK_SIZE;

  MB = azure.Constants.MB;

  HOUR_IN_MILLISECONDS = 60000 * 60;

  BlobTransport = (function(superClass) {
    extend(BlobTransport, superClass);

    function BlobTransport(arg) {
      var ref, ref1;
      this.account = arg.account, this.containerName = arg.containerName, this.blobName = arg.blobName, this.maxBlobSize = arg.maxBlobSize, this.maxBlockCount = (ref = arg.maxBlockCount) != null ? ref : 48000, this.level = (ref1 = arg.level) != null ? ref1 : "info";
      this._buildClient = bind(this._buildClient, this);
      this._meta = bind(this._meta, this);
      this._formatLine = bind(this._formatLine, this);
      this._retryIfNecessary = bind(this._retryIfNecessary, this);
      this._buildCargo = bind(this._buildCargo, this);
      this.log = bind(this.log, this);
      this.createNewBlobIfMaxSize = bind(this.createNewBlobIfMaxSize, this);
      this.rollBlob = bind(this.rollBlob, this);
      this.maxBlobSize = this.maxBlobSize ? this.maxBlobSize * MB : void 0;
      this.origBlobName = this.blobName;
      this.blobName = this.maxBlobSize ? this.blobName + '-' + this._timestamp() : this.origBlobName;
      this.name = "BlobTransport";
      this.cargo = this._buildCargo();
      this.client = this._buildClient(this.account);
      this.createNewBlobIfMaxSize();
    }

    BlobTransport.prototype.rollBlob = function() {
      var instance;
      instance = this;
      return instance.blobName = instance.origBlobName + '-' + instance._timestamp();
    };

    BlobTransport.prototype.createNewBlobIfMaxSize = function() {
      var instance;
      instance = this;
      return setInterval(function() {
        return instance.client.listBlobsSegmentedWithPrefix(instance.containerName, instance.blobName, null, function(err, result, response) {
          if ((err != null)) {

          } else if (result && result.entries[0] && result.entries[0].contentLength >= instance.maxBlobSize) {
            return instance.rollBlob();
          }
        });
      }, HOUR_IN_MILLISECONDS);
    };

    BlobTransport.prototype.initialize = function() {
      return Promise.promisifyAll(azure.createBlobService(this.account.name, this.account.key)).createContainerIfNotExistsAsync(this.containerName, {
        publicAccessLevel: "blob"
      }).then((function(_this) {
        return function(created) {
          return debug("Container: " + _this.container + " - " + (created ? 'creada' : 'existente'));
        };
      })(this));
    };

    BlobTransport.prototype.log = function(level, msg, meta, callback) {
      var line;
      line = this._formatLine({
        level: level,
        msg: msg,
        meta: meta
      });
      this.cargo.push({
        line: line,
        callback: callback
      });
    };

    BlobTransport.prototype._buildCargo = function() {
      var instance;
      instance = this;
      return async.cargo((function(_this) {
        return function(tasks, __whenFinishCargo) {
          var __whenLogAllBlock, chunks, logBlock;
          __whenLogAllBlock = function() {
            debug("Finish append all lines to blob");
            _.each(tasks, function(arg) {
              var callback;
              callback = arg.callback;
              return callback(null, true);
            });
            return __whenFinishCargo();
          };
          debug("Log " + tasks.length + "th lines");
          logBlock = _.map(tasks, "line").join("");
          debug("Starting append log lines to blob. Size " + logBlock.length);
          chunks = chunk(logBlock, MAX_BLOCK_SIZE);
          debug("Saving " + chunks.length + " chunk(s)");
          return async.eachSeries(chunks, function(chunk, whenLoggedChunk) {
            debug("Saving log with size " + chunk.length);
            return _this.client.appendFromText(_this.containerName, _this.blobName, chunk, function(err, result) {
              if (err) {
                return _this._retryIfNecessary(err, chunk, whenLoggedChunk);
              }
              if (result.committedBlockCount >= instance.maxBlockCount) {
                instance.rollBlob();
              }
              return whenLoggedChunk();
            });
          }, function(err) {
            if (err) {
              debug("Error in block");
            }
            return __whenLogAllBlock();
          });
        };
      })(this));
    };

    BlobTransport.prototype._retryIfNecessary = function(err, block, whenLoggedChunk) {
      var __createAndAppend, __doesNotExistFile, __handle;
      __createAndAppend = (function(_this) {
        return function() {
          return _this.client.createAppendBlobFromText(_this.containerName, _this.blobName, block, {}, __handle);
        };
      })(this);
      __doesNotExistFile = function() {
        return (err.code != null) && err.code === "NotFound";
      };
      __handle = function(err) {
        if (err) {
          debug("Error in append", err);
        }
        return whenLoggedChunk();
      };
      if (__doesNotExistFile()) {
        return __createAndAppend();
      } else {
        return __handle(err);
      }
    };

    BlobTransport.prototype._formatLine = function(arg) {
      var level, meta, msg;
      level = arg.level, msg = arg.msg, meta = arg.meta;
      return "[" + level + "] - " + (this._timestamp()) + " - " + msg + " " + (this._meta(meta)) + " \n";
    };

    BlobTransport.prototype._timestamp = function() {
      return new Date().toISOString();
    };

    BlobTransport.prototype._meta = function(meta) {
      if (meta instanceof Error) {
        meta = errorToJson(meta);
      }
      if (_.isEmpty(meta)) {
        return "";
      } else {
        return "- " + (util.inspect(meta));
      }
    };

    BlobTransport.prototype._buildClient = function(arg) {
      var key, name;
      name = arg.name, key = arg.key;
      return azure.createBlobService(name, key);
    };

    return BlobTransport;

  })(Transport);

  winston.transports.AzureBlob = BlobTransport;

  module.exports = BlobTransport;

}).call(this);
