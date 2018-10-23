debug = require("debug")("winston-blob-transport")
blobFinder = require('./blobFinder');
_ = require "lodash"
util = require "util"
errorToJson = require "error-to-json"
azure = require "azure-storage"
async = require "async"
winston = require "winston"
chunk = require "chunk"
Promise = require "bluebird"

Transport = winston.Transport

MAX_BLOCK_SIZE = azure.Constants.BlobConstants.MAX_APPEND_BLOB_BLOCK_SIZE;

MB = azure.Constants.MB;

HOUR_IN_MILLISECONDS = 60000 * 60;


###
  Given a list of objects containing {line: 'some text here', callback: {{some function here}}}
  returns a new array of the form:

  {
    length: 0
    text: ''
    callbacks: []
  }

  Length    -  the total length of the chunked log lines
  Text      -  the concatenated text of all the chunked text
  callbacks -  the concatenated callbacks of all chunked tasks

  Each entry in this new array represents one or more of the objects in the original array concatenated together, each entry is guaranteed to 
  have a length less than or equal to the MAX_BLOCK_SIZE global variable
###
chunkTasksBySize = (taskArray) ->
  if taskArray.length == 0
    return []

  # Initialize our new task array. 
  # Length    -  the total length of the chunked log lines
  # Text      -  the concatenated text of all the chunked text
  # callbacks -  the concatenated callbacks of all chunked tasks
  newTaskArray = [{
    length: 0
    text: ''
    callbacks: []
  }]

  for task in taskArray
    # the latest task added to our new array
    curTask = newTaskArray[newTaskArray.length - 1]

    # if adding this task to the latest task to our new array, makes it too large. Create a new one
    if curTask.length + task.line.length > MAX_BLOCK_SIZE
      curTask =
        length: 0
        text: ''
        callbacks: []
      newTaskArray.push curTask

    curTask.length += task.line.length
    curTask.text += task.line
    curTask.callbacks.push task.callback
  newTaskArray

###
  Allows winston to log to azure blob storage.
  Note For Multiple Instances:
    When logging in a multi instance environment (ie: The same parameters are supplied to this constructor for multiple instances) 
    logs will be written to the same blob log. However when the blob reaches its max size one instance will obtain a lock on the blob and generate a new one.
    All other instances will wait untill the creation of that new blob is done before attempting to log again. 
###
class BlobTransport extends Transport

  constructor: ({@account, @containerName, @blobName, @maxBlobSize, @maxBlockCount = 48000 , @level = "info"}) ->
    @maxBlobSize = if @maxBlobSize then (Math.round(@maxBlobSize * MB)) else undefined
    @origBlobName = @blobName
    @blobName = undefined
    @name = "BlobTransport"
    @cargo = @_buildCargo()
    @client = @_buildClient @account
    @client.acquireLease = Promise.promisify(this.client.acquireLease);
    @client.releaseLease = Promise.promisify(this.client.releaseLease);
    @client.createContainerIfNotExists = Promise.promisify(this.client.createContainerIfNotExists);
    @client.createAppendBlobFromText = Promise.promisify(this.client.createAppendBlobFromText);

    # prevent logs from attempting to write to blob storage untill we initialize
    @cargo.pause()

    # initialize the container and logger then allow logs to be stored to blob storage
    instance = this;
    @initialize()
    .then ()->
      instance.cargo.resume()

  # Attempts to create a new blob log.This function is safe for multiple instances of the app to use, 
  # since it will first attempt to obtain a lock on the previous blob before actually rolling (unless forceRoll is true).
  # 
  # The flow of operation is:
  #   1) Prevent anymore writes to blob storage and get the latest blob 
  #   2) If the latest blob is the same as the one we currently have, aquire a lease on it. Otherwise reenable writing to the blob
  #   3) If we were able to get a lease OR forceRoll was set to true create a new blob. Otherwise continue
  #   4) If we still have a lease on an old blob release the lease (essentially informs other instances that rolling is finished)
  #   5) Re-enable writing to blob storage. Note that if another process was creating a blob then on the next log attempt we will end up running this function again.
  rollBlob: (forceRoll)=>
    instance = this;

    # store the old blob as we will need it to delete our lock on the old blob.
    oldBlobName = instance.blobName
    leaseId = undefined;
    
    # prevent any new logs from attempting to write to blob storage
    instance.cargo.pause()

    # Aquire a lock on the last blob so that other instances know that this instance is creating a new blob
    rollPromise = instance.getCurBlob(true)
    .then ()->
      if (instance.blobName is oldBlobName)
        return instance.client.acquireLease(instance.containerName, oldBlobName, {leaseDuration:15})
      else
        # someone else already created a new blob since we found one newer than the one we had. Set the state to available
        instance.cargo.resume();
        return undefined;
    .catch (err)->
      debug('Could not get lease for current blob, another process is likely creating a new blob already ', err)
      return undefined;
    .then (lease)->
      leaseId = if lease? then lease.id else undefined
      if (leaseId? or forceRoll is true)
        newBlobName = instance.origBlobName + '-' + instance._timestamp();
        return instance.client.createAppendBlobFromText(instance.containerName, newBlobName, '[BLOB_ROLL_OVER] - previous blob: ' + oldBlobName + '\n')
      else
        return undefined;
    .then (newBlob)->
      if (newBlob != undefined)
        # we created a new blob up above store the new blob and set the state to available
        instance.blobName = newBlob.name;
      if leaseId?
        return instance.client.releaseLease(instance.containerName, oldBlobName, leaseId) 
      else 
        return undefined;
    .then () ->
      instance.cargo.resume();
    .catch (err)->
      debug('Something went wrong returning old blob ', err)
      return instance.blobName;

  initialize: ->
     instance = this
     debug 'initializing winston logger'
     return instance.client.createContainerIfNotExists instance.containerName, publicAccessLevel: "blob"
      .then (container) -> 
        if container.created?
          debug 'Container ' + instance.containerName + ' already exists' 
        else 
          debug 'Container ' + instance.containerName + ' does not exist, creating.' 
      .then () -> 
        return instance.getCurBlob true
      .then (curBlob) ->
        if curBlob?
          debug 'Existing blobs found for ' + instance.origBlobName + ' not creating new one'
        else
          debug 'Existing blobs not found for ' + instance.origBlobName + ' creating new one'
          return instance.rollBlob true


  getCurBlob: (forceUpdate) => 
    instance = this;

    # Refresh our blob if the caller explicitly tells us to search for a new one OR the state of the blob was previously not available.
    if(forceUpdate || instance.cargo.paused)
      return blobFinder.searchLatestBlob(instance.client,instance.containerName, instance.origBlobName)
      .then (result)->
          if result?
            instance.blobName = result.name
            return instance.blobName;
          else 
            return undefined
     else 
        return Promise.resolve(instance.blobName);

  log: (level, msg, meta, callback) =>
    instance = this
    line = instance._formatLine {level, msg, meta}
    instance.cargo.push { line, callback }

  _buildCargo: =>
    instance = this;
    async.cargo (tasks, __whenFinishCargo) =>

      __whenLogAllBlock = ->
        debug "Finish append all lines to blob"
        __whenFinishCargo()

      debug "Log #{tasks.length} lines"
      logBlock = _.map(tasks, "line").join ""
      taskObjects = chunkTasksBySize tasks

      # refresh the current blob
      blobStatusProm = instance.getCurBlob()    
      .then ()->
        # we now have the latest and greatest blob
        async.eachOfSeries taskObjects, (chunk, index , whenLoggedChunk) =>
          debug "Logging chunk #{index}"
          instance.client.appendBlockFromText  instance.containerName,  instance.blobName, chunk.text, { maxBlobSize: instance.maxBlobSize }, (err, result) =>
            debug "Finish saving log chunk result was:" + result;
            if (err)
              whenLoggedChunk {underlyingError: err, failedIndex: index}
            else
              # call all of this chunks callbacks
              _.each chunk.callbacks, (callback) -> callback null, true 
              whenLoggedChunk()

        , (err) ->
          if err
            debug "One of the chunks enountered an error readding all remaining chunks to the queue"
            # start at the failed log and add it as well as all remaining logs back to the queue
            for index in [err.failedIndex...taskObjects.length]
              task = taskObjects[index]
              line = task.text
              callback = () -> task.callbacks.map (nextCallback)->nextCallback()
              instance.cargo.unshift {line , callback }

          # if there was an error and it was related to the blob being full OR a blob having a lease on this blob attempt to roll to a new blob
          if (err? && err.underlyingError? && (err.underlyingError.code is "MaxBlobSizeConditionNotMet" or err.underlyingError.code is "LeaseIdMissing"))
              debug "The blob has reached its max size attempting to create a new blob"
              instance.rollBlob()
              .then (result) ->
                __whenLogAllBlock()
          else 
            __whenLogAllBlock();

  _formatLine: ({level, msg, meta}) => "[#{level}] - #{@_timestamp()} - #{msg} #{@_meta(meta)} \n"

  _timestamp: -> new Date().toISOString()

  _meta: (meta) =>
    meta = errorToJson meta if meta instanceof Error
    if _.isEmpty meta then "" else "- #{util.inspect(meta)}"

  _buildClient : ({name, key}) =>
    azure.createBlobService name, key

#
# Define a getter so that `winston.transports.AzureBlob`
# is available and thus backwards compatible.
#
winston.transports.AzureBlob = BlobTransport

module.exports = BlobTransport