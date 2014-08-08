
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'write', ->

  it 'using arguments', decorate (next) ->
    location = "#{config.basedir}/a_file"
    client = hdfs(config)
    client.write location, 'some content', (err) ->
      return next err if err
      client.read location, (err, data) ->
        return next err if err
        data.should.eql 'some content'
        next()

  it 'using options', decorate (next) ->
    location = "#{config.basedir}/a_file"
    client = hdfs(config)
    client.write path: location, data: 'some content', (err, body) ->
      return next err if err
      client.read location, (err, data) ->
        return next err if err
        data.should.eql 'some content'
        next()

  it 'using options', decorate (next) ->
    location = "#{config.basedir}/a_file"
    client = hdfs(config)
    client.write path: location, data: 'some content', replication: 1, (err, body) ->
      return next err if err
      client.read location, (err, data) ->
        return next err if err
        data.should.eql 'some content'
        next()