
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'list', ->

  it 'a directory', decorate (next) ->
    location = "/"
    hdfs(config).list location, (err, stats) ->
      return next err if err
      for stat in stats.FileStatuses.FileStatus
        return next() if stat.pathSuffix is 'user'
      next Error 'Directory "user" not found inside "/"'