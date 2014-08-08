
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'remove', ->

  it 'a directory in user home', decorate (next) ->
    location = "/#{config.basedir}/a_dir"
    client = hdfs(config)
    client.mkdir location, (err) ->
      return next err if err
      client.remove location, (err) ->
        return next err if err
        client.stat location, (err, stat) ->
          err.code.should.eql 404
          err.message.should.eql '404 Not Found'
          next()
