'use strict';

var APIError = require('./error');

var tv4 = require('tv4');
var async = require('async');
var querystring = require('querystring');
var fs = require('fs');
var path = require('path');
var url = require('url');
var http = require('http');
var https = require('https');
var pem = require('pem');

/**
 * List of supported operations
 * @type {string[]}
 */
var supportedOperations = [
  'mkdirs',
  'rename',
  'append',
  'create',
  'open',
  'delete',
  'getfilestatus',
  'setpermission',
  'setowner',
  'liststatus',
  'createsymlink'

];

/**
 * List of operations redirected to datanode
 * @type {string[]}
 */
var redirectedOperations = [
  'create',
  'append',
  'open'
];

/**
 *
 * @param {Object} opts Server options
 * @param {Object} opts.host Local hostname
 * @param {Object} opts.path Path prefix
 * @param {Object} opts.http HTTP server options
 * @param {Object} opts.https HTTPS server options
 * @param {WebHDFSProxyServer~MiddlewareHandler} middleware Middleware handler
 * @constructor
 */
function WebHDFSProxyServer (opts, middleware) {
  this._servers = [];
  this._handler = middleware;
  this._opts = opts;
  this._path = opts.path || '/webhdfs/v1';
  this._host = opts.host || 'localhost';
  this._validate = opts.hasOwnProperty('validate') ? opts.validate : true;

  var self = this;

  // Handle request
  var listen = function listen (req, res) {
    self._handleRequest(req, res, function requestFinished (err, data) {
      // Serialize and send RemoteException data structure
      if (err && err instanceof APIError) {
        var serializedError = err.toJSON();

        res.writeHead(err.statusCode, {
          'content-type': 'application/json',
          'content-length': serializedError.length
        });

        res.end(serializedError);
      } else {
        res.writeHead(200);
        res.end();
      }
    });
  };

  // HTTP server
  if (this._opts.hasOwnProperty('http')) {
    if (!this._opts.http.hasOwnProperty('port')) {
      this._opts.http.port = 80;
    }

    var server = http.createServer(listen);
    server.listen(this._opts.http.port);

    this._servers.push(server);
  }

  // TODO: Implement HTTPS server
  if (opts.hasOwnProperty('https')) { }
}

/**
 * Incoming WebHDFS request handler
 *
 * @param {Object} req Request object handle
 * @param {Object} res Response object handle
 */
WebHDFSProxyServer.prototype._handleRequest = function handleRequest (req, res, next) {
  var self = this;

  /**
   * Finalize request if middleware is done
   * Generates APIError if middleware passes error object back
   *
   * @param {Error} [err] Optional error
   */
  function done (err) {
    if (err) {
      if (err instanceof APIError) {
        return next(err);
      } else {
        return next(
          new APIError('RuntimeException', { message: err.message })
        );

      }
    } else {
      return next();
    }
  }

  // Accept only valid requests
  if (req.url.indexOf(this._path) === 0) {
    var queryStringPosition = req.url.indexOf('?');

    // Parse request path
    req.path = req.url.substring(this._path.length, queryStringPosition || req.url.length);
    req.params = queryStringPosition !== -1 ? querystring.parse(req.url.substring(queryStringPosition + 1)) : {};
    req.params.op = req.params.op && req.params.op.toLowerCase();

    // Validate operation
    if (supportedOperations.indexOf(req.params.op) === -1) {
      self._handler(
        new APIError('UnsupportedOperationException', {
          operation: req.params.op
        }), req, res, done);

      return;
    }

    // Validate request parameters
    if (this._validate) {
      this._validateRequest(req.params.op, req.params, function requestValidated (err) {
        if (err) {
          if (err.code && err.code === 'MODULE_NOT_FOUND') {
            self._handler(
              new APIError('RuntimeException', {
                message: 'Unable to validate request path: ' + req.url
              }), req, res, done);
          } else {
            self._handler(
              new APIError('IllegalArgumentException', {
                parameter: err.dataPath && err.dataPath.substring(1),
                message: err.message
              }), req, res, done);
          }
        } else {
          // Handle all possible datanode redirections
          if (req.params.hasOwnProperty('redirect')) {
            self._handler(null, req, res, done);
          } else {
            if (redirectedOperations.indexOf(req.params.op) !== -1) {
              var isSecure = req.connection.hasOwnProperty('encrypted');
              var redirectURL = url.format({
                protocol: isSecure ? 'https:' : 'http:',
                port: isSecure ? this._opts.https.port : this._opts.http.port,
                hostname: this._host,
                pathname: this._path + req.path,
                search: req.url.substring(queryStringPosition + 1) + '&redirect=true'
              });

              res.writeHead(307, { 'location': redirectURL });
              res.end();
            } else {
              self._handler(null, req, res, done);
            }
          }
        }
      });
    } else {
      this._handler(null, req, res, done);
    }
  } else {
    return next(
      new APIError('RuntimeException', {
        message: 'Invalid request path: ' + req.url
      })
    );
  }
};

/**
 * Validate operation parameters against JSON schema
 *
 * @param {String} operation Operation name
 * @param {Object} params Request params
 * @param {Function} done
 */
WebHDFSProxyServer.prototype._validateRequest = function validateRequest (operation, params, done) {
  var self = this;

  async.waterfall([
    function loadSchema (next) {
      var schema = null;

      try {
        schema = require(path.resolve('schemas/', operation + '.json'));
        return next(null, schema);
      } catch (err) {
        return next(err);
      }
    },

    function validateParams (schema, next) {
      var result = tv4.validateResult(params, schema);
      if (result.valid) {
        return next();
      } else {
        return next(result.error);
      }
    }
  ], function onEnd (err) {
    return done(err);
  });
};

/**
 * Middleware handler function
 *
 * @callback WebHDFSProxyServer~MiddlewareHandler
 * @param {Object} req Request object handle
 * @param {Object} res Response object handle
 */

module.exports = WebHDFSProxyServer;