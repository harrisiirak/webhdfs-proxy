'use strict';

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
  this._listen = this._handleRequest.bind(this);

  // HTTP server
  if (opts.hasOwnProperty('http')) {
    if (!opts.http.hasOwnProperty('port')) {
      opts.http.port = 80;
    }

    var server = http.createServer(this._listen);
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
WebHDFSProxyServer.prototype._handleRequest = function handleRequest (req, res) {
  function done (err) {

  }

  // Accept only valid requests
  if (req.url.indexOf(this._path) === 0) {
    var queryStringPosition = req.url.indexOf('?');

    // Parse request path
    req.path = req.url.substring(this._path.length, queryStringPosition || req.url.length);
    req.params = queryStringPosition !== -1 ? querystring.parse(req.url.substring(queryStringPosition + 1)) : {};

    // TODO: Validate request parameters

    this._handler(null, req, res, done);
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