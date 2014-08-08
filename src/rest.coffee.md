
# HTTP REST Utils

    module.exports = rest = (options) ->
      new Rest options
    module.exports.Rest = Rest
    module.exports.CURLAUTH_GSSNEGOTIATE = 4

## REST class and constructor

    class Rest
      constructor: (@options) ->
        @options.port ?= 50070

## REST GET

      get: (options, callback) ->
        return callback Error 'Options "path" missing' unless options.path?
        return callback Error 'Options "query" missing' unless options.query?
        return callback Error 'Options "query.op" missing' unless options.query.op?
        {hostname, port, protocol, doas} = @options
        options.query.doas = doas if doas
        options.code ?= [200]
        options.code = [options.code] unless Array.isArray options.code
        options.url ?= url.format
          protocol: protocol
          hostname: hostname
          port: port
          pathname: path.join '/webhdfs/v1', options.path
          query: options.query
        curl options.url,
          CUSTOMREQUEST: 'GET'
          USERPWD: ':' if @options.secured
          HTTPAUTH: if @options.secured then rest.CURLAUTH_GSSNEGOTIATE else 0
          VERBOSE: @options.verbose
          DEBUG: @options.debug
          FOLLOWLOCATION: options.follow
        , (err) ->
          if options.code.indexOf(@code) is -1
            err = Error "#{@code} #{status[@code]}"
            err.code = @code
            callback err
          else
            callback null, @, @body

## REST PUT

      put: (options, callback) ->
        unless options.url
          return callback Error 'Options "path" missing' unless options.path?
          return callback Error 'Options "query" missing' unless options.query?
          return callback Error 'Options "query.op" missing' unless options.query.op?
        {hostname, port, protocol, doas} = @options
        options.query.doas = doas if doas
        options.code ?= [200, 201]
        options.code = [options.code] unless Array.isArray options.code
        options.url ?= url.format
          protocol: protocol
          hostname: hostname
          port: port
          pathname: path.join '/webhdfs/v1', options.path
          query: options.query
        coptions = 
          CUSTOMREQUEST: 'PUT'
          USERPWD: ':' if @options.secured
          HTTPAUTH: if @options.secured then rest.CURLAUTH_GSSNEGOTIATE else 0
          VERBOSE: @options.verbose
          DEBUG: @options.debug
        coptions.POSTFIELDS = options.data if options.data
        curl options.url, coptions, (err, res) ->
          if options.code.indexOf(@code) is -1
            err = Error "#{@code} #{status[@code]}"
            err.code = res.code
            callback err, res
          else
            callback null, res, res.body

## REST POST

      post: (options, callback) ->
        unless options.url
          return callback Error 'Options "path" missing' unless options.path?
          return callback Error 'Options "query" missing' unless options.query?
          return callback Error 'Options "query.op" missing' unless options.query.op?
        {hostname, port, protocol, doas} = @options
        options.query.doas = doas if doas
        options.code ?= [200, 201]
        options.code = [options.code] unless Array.isArray options.code
        options.url ?= url.format
          protocol: protocol
          hostname: hostname
          port: port
          pathname: path.join '/webhdfs/v1', options.path
          query: options.query
        coptions = 
          CUSTOMREQUEST: 'POST'
          USERPWD: ':' if @options.secured
          HTTPAUTH: if @options.secured then rest.CURLAUTH_GSSNEGOTIATE else 0
          VERBOSE: @options.verbose
          DEBUG: @options.debug
        coptions.POSTFIELDS = options.data if options.data
        curl options.url, coptions, (err) ->
          if options.code.indexOf(@code) is -1
            err = Error "#{@code} #{status[@code]}"
            err.code = @code
            callback err, @
          else
            callback null, @, @body

## REST Delete

      del: (options, callback) ->
        return callback Error 'Options "path" missing' unless options.path?
        return callback Error 'Options "query" missing' unless options.query?
        return callback Error 'Options "query.op" missing' unless options.query.op?
        {hostname, port, protocol, doas} = @options
        options.query.doas = doas if doas
        options.code ?= [200]
        options.code = [options.code] unless Array.isArray options.code
        u = url.format
          protocol: protocol
          hostname: hostname
          port: port
          pathname: path.join '/webhdfs/v1', options.path
          query: options.query
        curl u,
          CUSTOMREQUEST: 'DELETE'
          USERPWD: ':' if @options.secured
          HTTPAUTH: if @options.secured then rest.CURLAUTH_GSSNEGOTIATE else 0
          VERBOSE: @options.verbose
          DEBUG: @options.debug
        , (err) ->
          if options.code.indexOf(@code) is -1
            err = Error "#{@code} #{status[@code]}"
            err.code = @code
            callback err
          else
            callback null, @, @body

## Module Dependencies

    path = require 'path'
    url = require 'url'
    status = require 'http-status'
    curl = require 'node-curl'