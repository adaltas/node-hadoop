
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'symlink', ->

  it 'using arguments', decorate (next) ->
    source = "#{config.basedir}/nodejs"
    symlink = "#{config.basedir}/nodejs_symlink"
    client = hdfs(config)
    client.write source, 'some content', (err) ->
      return next err if err
      client.symlink source, symlink, (err, data) ->
        return dispose() if err?.message is 'Symlinks not supported'
        return next err if err
        client.stat symlink, (err, stat) ->
          return next err if err
          dispose()
    dispose = (e) ->
      client.remove source, (err) ->
        return next err if err
        client.remove symlink, (err) ->
          return next err if err
          next e