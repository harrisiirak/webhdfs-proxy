'use strict';

var APIError = require('./error');

var JaySchema = require('jayschema');
var querystring = require('querystring');
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
  this._validator = this._validate ? new JaySchema() : null;

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

    // TODO: Validate request parameters
    this._handler(null, req, res, done);
  } else {
    return next(
      new APIError('RuntimeException', {
        message: 'Invalid request path: ' + req.url
      })
    );
  }
};

/**
 * Middleware handler function
 *
 * @callback WebHDFSProxyServer~MiddlewareHandler
 * @param {Object} req Request object handle
 * @param {Object} res Response object handle
 */

module.exports = WebHDFSProxyServer;