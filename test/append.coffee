
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'append', ->

  it 'using arguments', decorate (next) ->
    @timeout 10000
    client = hdfs(config)
    client.write path: "#{config.basedir}/a_file", data: 'some', replication: 1, (err) ->
      return next err if err
      client.append "#{config.basedir}/a_file", ' appended', (err) ->
        return next err if err
        client.append "#{config.basedir}/a_file", ' content', (err) ->
          return next err if err
          client.read "#{config.basedir}/a_file", (err, data) ->
            return next err if err
            data.should.eql 'some appended content'
            next()