
# Hadoop HDFS driver

    module.exports = (options) ->
      new HDFS options
    module.exports.HDFS = HDFS

## HDFS class and constructor

    class HDFS
      constructor: (@options) ->
        @options.protocol ?= 'http'
        @options.port ?= 50070
        @options.doas ?= null
        @options.verbose ?= false
        @options.debug ?= false
        @rest = new rest @options

## Create and Write to a File

`write(path, data, [overwrite], [blocksize], [replication], [permission], [buffersize], callback)`
`write(options, callback)`

      write: (path, data, overwrite, blocksize, replication, permission, buffersize, callback) ->
        # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
        query_keys = ['overwrite', 'blocksize', 'replication', 'permission', 'buffersize']
        [options, callback] = misc.args arguments, ['path', 'data'].concat query_keys...
        query = op: 'CREATE'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.put path: options.path, query: query, code: [307, 201], (err, res) =>
          return callback err if err
          return callback null, res if res.code is 201
          headers = res.header.split /\r\n/
          for h in headers
            if match = /^Location: (.*)$/.exec h
              location = match[1]
              break
          @rest.put url: location, data: options.data, (err, res, body) ->
            callback err, res

## Append to a File

`append(path, [buffersize], callback)`
`append(options, callback)`

      append: (path, data, buffersize, callback) ->
        # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>]"
        # curl -i -X POST -T <LOCAL_FILE> "http://<DATANODE>:<PORT>/webhdfs/v1/<PATH>?op=APPEND..."
        query_keys = ['buffersize']
        [options, callback] = misc.args arguments, ['path', 'data'].concat query_keys...
        query = op: 'APPEND'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.post path: options.path, query: query, code: 307, (err, res) =>
          return callback err if err
          headers = res.header.split /\r\n/
          for h in headers
            if match = /^Location: (.*)$/.exec h
              location = match[1]
              break
          @rest.post url: location, data: options.data, (err, res, body) ->
            callback err

## Concat

`concat(path, [buffersize], callback)`
`concat(options, callback)`

      concat: (path, sources, callback) ->
        # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=CONCAT&sources=<PATHS>"
        query_keys = ['sources']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        options.sources = options.sources.join ',' if Array.isArray options.sources
        query = op: 'CONCAT'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.post path: options.path, query: query, (err, res) =>
          callback err, res

## Open and Read a File

`read(path, [offset], [length], [buffersize], callback)`
`read(options, callback)`

      read: (path, offset, length, buffersize, callback) ->
        # curl -i -L "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=OPEN[&offset=<LONG>][&length=<LONG>][&buffersize=<INT>]"
        query_keys = ['offset', 'length', 'buffersize']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'OPEN'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.get path: options.path, query: query, follow: true, code: 200, (err, res, data) =>
          return callback err if err
          callback null, data

## Make a Directory

`mkdir(path, [permission], callback)`
`mkdir(options, callback)`

The callback receives an error and a raw response with a boolean JSON object.

      mkdir: (path, permission, callback) ->
        # curl -i -X PUT "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=MKDIRS[&permission=<OCTAL>]"
        query_keys = ['permission']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'MKDIRS'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.put path: options.path, query: query, (err, res, body) ->
          return callback err if err
          body = JSON.parse body
          try callback null, body
          catch err then callback err

## Create a Symbolic Link

`symlink(path, destination, [createParent], callback)`
`symlink(options, callback)`

The callback receives an error and a raw response with a boolean JSON object.

      symlink: (path, destination, createParent, callback) ->
        # curl -i -X PUT "http://<HOST>:<PORT>/<PATH>?op=CREATESYMLINK&destination=<PATH>[&createParent=<true|false>]"
        query_keys = ['destination', 'createParent']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'CREATESYMLINK'
        for k in query_keys then query[k] = options[k] if options[k]
        query.createParent = if query.createParent then 'true' else 'false'
        @rest.put path: options.path, query: query, (err, res) ->
          try
            body = JSON.parse res.body if res.body
            err = Error body.RemoteException?.message if err and body.RemoteException?.message
            callback err, res
          catch e then callback err or e

## Rename a File/Directory

`rename(path, destination, callback)`
`rename(options, callback)`

The callback receives an error and a raw response with a boolean JSON object.

      rename: (path, destination, callback) ->
        # curl -i -X PUT "<HOST>:<PORT>/webhdfs/v1/<PATH>?op=RENAME&destination=<PATH>"
        query_keys = ['destination']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        return callback Error 'Required argument "destination"' unless options.destination
        query = op: 'RENAME'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.put path: options.path, query: query, (err, res) ->
          try
            body = JSON.parse res.body if res.body
            err = Error body.RemoteException?.message if err and body.RemoteException?.message
            callback err, res
          catch e then callback err or e

## Delete a File/Directory

`remove(path, [recursive], callback)`
`remove(options, callback)`

      remove: (path, recursive, callback) ->
        # curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE[&recursive=<true|false>]"
        query_keys = ['recursive']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'DELETE'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.del path: options.path, query: query, (err, res, body) ->
          callback err

## Status of a File/Directory

`stat(path, callback)`
`remove(options, callback)`

      stat: (path, callback) ->
        # curl -i "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=GETFILESTATUS"
        query_keys = ['recursive']
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'GETFILESTATUS'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.get path: options.path, query: query, (err, res, body) ->
          return callback err if err
          try callback null, JSON.parse body
          catch err then callback err

## List a Directory

`list(path, callback)`
`remove(options, callback)`

The callback receives an error or a response with a 
[FileStatuses JSON object][FileStatuses].

      list: (path, callback) ->
        # curl -i  "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=LISTSTATUS"
        query_keys = []
        [options, callback] = misc.args arguments, ['path'].concat query_keys...
        query = op: 'LISTSTATUS'
        for k in query_keys then query[k] = options[k] if options[k]
        @rest.get path: options.path, query: query, (err, res, body) ->
          return callback err if err
          try callback null, JSON.parse body
          catch err then callback err

## Module Dependencies

    misc = require './misc'
    rest = require './rest'

[FileStatuses]: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#FileStatuses

