'use strict';

var APIError = require('./error');

var tv4 = require('tv4');
var async = require('async');
var querystring = require('querystring');
var fs = require('fs');
var path = require('path');
var http = require('http');
var https = require('https');
var pem = require('pem');

/**
 *
 * @param {Object} opts Server options
 * @param {Object} opts.http HTTP server options
 * @param {Object} opts.https HTTPS server options
 * @param {WebHDFSProxyServer~MiddlewareHandler} middleware Middleware handler
 * @constructor
 */
function WebHDFSProxyServer (opts, middleware) {
  this._servers = [];
  this._handler = middleware;
  this._path = opts.path || '/webhdfs/v1';
  this._validate = opts.hasOwnProperty('validate') ? opts.validate : true;

  var self = this;

  // Handle request
  var listen = function listen (req, res) {
    self._handleRequest(req, res, function requestFinished (err) {
      // Serialize and send RemoteException data structure
      if (err && err instanceof APIError) {
        var serializedError = err.toJSON();

        res.writeHead(err.statusCode, {
          'content-type': 'application/json',
          'content-length': serializedError.length
        });

        res.end(serializedError);
      }
    });
  };

  // HTTP server
  if (opts.hasOwnProperty('http')) {
    if (!opts.http.hasOwnProperty('port')) {
      opts.http.port = 80;
    }

    var server = http.createServer(listen);
    server.listen(opts.http.port);

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

    // TODO: Validate operation
    // TODO: Validate request parameters
    if (this._validate) {
      this._validateRequest(req.params.op, req.params, function requestValidated (err) {
        self._handler(null, req, res, done);
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