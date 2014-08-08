
    module.exports = 
      args: (args, properties) ->
        if args.length is 2 and typeof args[0] is 'object' and not Array.isArray args[0]
          options = args[0]
          callback = args[1]
        else
          options = {}
          callback = args[args.length-1]
          for argument, i in Array.prototype.slice.call(args)[0...args.length-1]
            options[properties[i]] = argument
        [options, callback]
