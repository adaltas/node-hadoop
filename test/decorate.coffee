
hdfs = require '../src/hdfs'
config = require './config'

module.exports = (test) ->
  (next) ->
    self = @
    client = hdfs(config)
    client.mkdir config.basedir, (err) ->
      return next err if err
      test.call self, (test_err) ->
        client.remove path: config.basedir, recursive: true, (err) ->
          next test_err or err





