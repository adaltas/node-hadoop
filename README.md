

This project implement the [WebHDFS][webhdfs] protocol which supports the complete FileSystem interface for HDFS with a REST API.

Usage
-----

Installation command is `npm install hadoop`.

Development
-----------

Tests are executed with mocha. To install it, simple run `npm install`, it will install
mocha and its dependencies in your project "node_modules" directory.

To run the tests:
```bash
npm test
```

The tests run against the CoffeeScript source files.

To generate the JavaScript files:
```bash
make build
```

Contributors
------------

*   David Worms: <https://github.com/wdavidw>

[webhdfs]: http://hadoop.apache.org/docs/r1.0.4/webhdfs.html

