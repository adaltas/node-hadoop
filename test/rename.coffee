
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'rename', ->

  it 'using arguments', decorate (next) ->
    location =
    client = hdfs(config)
    client.write "#{config.basedir}/org_file", 'some content', (err) ->
      return next err if err
      client.rename "#{config.basedir}/org_file", "#{config.basedir}/new_file", (err) ->
        return next err if err
        client.read "#{config.basedir}/new_file", (err, data) ->
          return next err if err
          data.should.eql 'some content'
          next()
