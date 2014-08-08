
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'stat', ->

  it 'a directory', decorate (next) ->
    hdfs(config).stat "#{config.basedir}", (err, stat) ->
      return next err if err
      stat.FileStatus.type.should.eql 'DIRECTORY'
      next()

  it 'a missing file', decorate (next) ->
    location = "#{config.basedir}/doesnotexists"
    hdfs(config).stat location, (err, stat) ->
      err.code.should.eql 404
      err.message.should.eql '404 Not Found'
      next()
