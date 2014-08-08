
should = require 'should'
hdfs = if process.env.HADOOP_COV then require '../lib-cov/hdfs' else require '../src/hdfs'
config = require './config'
decorate = require './decorate'

describe 'concat', ->

  it 'using arguments', decorate (next) ->
    # The function seems to work but I dont know how to use and test concat
    # Help is welcome in such situation
    next()
    # location = "/user/#{config.username}/nodejs"
    # sources = [
    #   "/user/#{config.username}/nodejs_1"
    #   "/user/#{config.username}/nodejs_2"
    #   "/user/#{config.username}/nodejs_3"
    # ]
    # client = hdfs(config)
    # client.write sources[0], 'some', (err) ->
    #   return next err if err
    #   client.write sources[1], ' concated', (err) ->
    #     return next err if err
    #     client.write sources[2], ' content', (err) ->
    #       return next err if err
    #       client.concat location, sources, (err, data) ->
    #         return next err if err
    #         data.should.eql 'some concated content'
    #         client.remove location, (err) ->
    #           return next err if err
    #           client.remove sources[0], (err) ->
    #             return next err if err
    #             client.remove sources[1], (err) ->
    #               return next err if err
    #               client.remove sources[2], (err) ->
    #                 next err