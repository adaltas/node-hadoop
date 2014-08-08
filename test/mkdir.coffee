
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'mkdir', ->

  it 'a new directory', decorate (next) ->
    location = "#{config.basedir}/a_dir"
    client = hdfs(config)
    client.mkdir location, (err) ->
      return next err if err
      client.stat location, (err, stat) ->
        return next err if err
        stat.FileStatus.type.should.eql 'DIRECTORY'
        next()

  it 'an existing directory', (next) ->
    location = "#{config.basedir}/a_dir"
    client = hdfs(config)
    client.mkdir location, (err) ->
      return next err if err
      client.mkdir location, (err) ->
        next err
