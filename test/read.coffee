
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'read', ->

  it 'a file', decorate (next) ->
    location = "#{config.basedir}/nodejs"
    client = hdfs(config)
    client.write location, 'some content', (err) ->
      return next err if err
      client.read location, (err, data) ->
        return next err if err
        data.should.eql 'some content'
        next()