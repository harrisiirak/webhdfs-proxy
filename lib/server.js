'use strict';

var APIError = require('./error').APIError;

var Promise = require('bluebird');
var pem = require('pem');
var validator = require('validator');
var qs = require('qs');
var tv4 = require('tv4');
var async = require('async');
var path = require('path');
var url = require('url');
var http = require('http');
var https = require('https');
var fs = require('fs');
var util = require('util');

/**
 * List of supported operations
 * @type {Object[]}
 */
var supportedOperations = {
  mkdirs: {
    method: 'PUT',
    redirect: false
  },

  rename: {
    method: 'PUT',
    redirect: false
  },

  append: {
    method: 'POST',
    redirect: true
  },

  create: {
    method: 'PUT',
    redirect: true
  },

  open: {
    method: 'GET',
    redirect: true
  },

  delete: {
    method: 'DELETE',
    redirect: false
  },

  getfilestatus: {
    method: 'GET',
    redirect: false
  },

  setpermission: {
    method: 'PUT',
    redirect: false
  },

  setowner: {
    method: 'PUT',
    redirect: false
  },

  liststatus: {
    method: 'GET',
    redirect: false
  },

  createsymlink: {
    method: 'PUT',
    redirect: false
  }

};

/**
 *
 * @param {Object} opts Server options
 * @param {Object} opts.host Local hostname
 * @param {Object} opts.path Path prefix
 * @param {Object} opts.http HTTP server options
 * @param {Object} opts.https HTTPS server options
 * @param {Function} done Called when server(s) initialization is done
 * @param {WebHDFSProxyServer~MiddlewareHandler} middleware Middleware handler
 * @constructor
 */
function WebHDFSProxyServer (opts, middleware, done) {
  this._servers = [];
  this._handler = middleware;
  this._opts = opts;
  this._path = opts.path || '/webhdfs/v1';
  this._host = opts.host || 'localhost';
  this._validate = opts.hasOwnProperty('validate') ? opts.validate : true;

  var self = this;

  // Handle request
  function listen (req, res) {
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
  }

  function createServer (opts, secure) {
    var server = null;
    if (opts === undefined) {
      return null;
    }

    if (secure) {
      if (!opts.hasOwnProperty('port')) {
        opts.port = 443;
      }

      if (!opts.hasOwnProperty('key') || !opts.hasOwnProperty('cert')) {
        var serverResolver = Promise.defer();

        pem.createCertificate({ days: 365, selfSigned: true }, function certificateCreated (err, keys) {
          if (err) {
            return serverResolver.reject(err);
          }

          server = https.createServer({
            key: keys.serviceKey,
            cert: keys.certificate
          }, listen);

          server.listen(opts.port);
          serverResolver.resolve(server);
        });

        return serverResolver.promise;
      } else {
        return Promise.attempt(function () {
          server = https.createServer({
            key: fs.readFileSync(opts.key),
            cert: fs.readFileSync(opts.cert)
          }, listen);

          server.listen(opts.port);
          return server;
        });
      }
    } else {
      return Promise.attempt(function () {
        if (!opts.hasOwnProperty('port')) {
          opts.port = 80;
        }

        server = http.createServer(listen);
        server.listen(opts.port);

        return server;
      });
    }
  }

  // Create servers
  Promise.all([
    createServer(this._opts.http, false),
    createServer(this._opts.https, true)
  ]).then(function resolved (servers) {
    // Return only valid server instances
    servers.forEach(function iterateServer (server) {
      if (server) {
        self._servers.push(server);
      }
    });

    return done(null, self._servers);
  }).catch(function rejected (err) {
    return done(err);
  }).error(function rejected (err) {
    return done(err);
  });
}

/**
 * Parses input query string and tries to cast values
 *
 * @param {String} queryString Input query string
 * @returns {Object}
 */
WebHDFSProxyServer.prototype._parseQueryString = function parseQueryString (queryString) {
  if (!queryString || !queryString.length || queryString.indexOf('&') === -1) {
    return {};
  }

  var params = qs.parse(queryString);

  // Type casting
  for (var key in params) {
    // Preserve octal valus
    if (params[key].length && params[key][0] === '0') {
      continue;
    }

    if (validator.isIn(params[key], [ 'true', 'false'])) {
      params[key] = validator.toBoolean(params[key], true);
    } else if (validator.isInt(params[key])) {
      params[key] = parseInt(params[key], 10);
    } else if (validator.isFloat(params[key])) {
      params[key] = parseFloat(params[key]);
    }
  }

  return params;
};

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
    var path = req.url.substring(this._path.length, queryStringPosition || req.url.length);
    var params = queryStringPosition !== -1 ? this._parseQueryString(req.url.substring(queryStringPosition + 1)) : {};
    var operation = params.op && params.op.toLowerCase();

    // Validate operation
    if (!supportedOperations.hasOwnProperty(operation)) {
      self._handler(
        new APIError('UnsupportedOperationException', {
          operation: operation
        }), path, operation, params, req, res, done);

      return;
    }

    // Validate HTTP method
    if (supportedOperations[operation].method !== req.method) {
      self._handler(
        new APIError('RuntimeException', {
          message: util.format('Invalid HTTP method %s, expected %s',
            req.method, supportedOperations[operation].method)
        }), path, operation, params, req, res, done);
      return;
    }

    // Validate request parameters
    if (this._validate) {
      this._validateRequest(operation, params, function requestValidated (err) {
        if (err) {
          if (err instanceof APIError) {
            self._handler(err, path, operation, params, req, res, done);
          } else if (err.code && err.code === 'MODULE_NOT_FOUND') {
            self._handler(
              new APIError('RuntimeException', {
                message: 'Unable to validate request path: ' + req.url
              }), path, operation, params, req, res, done);
          } else {
            self._handler(
              new APIError('IllegalArgumentException', {
                message: err.message
              }), path, operation, params, req, res, done);
          }
        } else {
          // Handle all possible datanode redirections
          if (params.hasOwnProperty('redirect')) {
            self._handler(null, path, operation, params, req, res, done);
          } else {
            if (supportedOperations[operation].redirect) {
              var isSecure = req.connection.hasOwnProperty('encrypted');
              var redirectURL = url.format({
                protocol: isSecure ? 'https:' : 'http:',
                port: isSecure ? self._opts.https.port : self._opts.http.port,
                hostname: self._host,
                pathname: self._path + path,
                search: req.url.substring(queryStringPosition + 1) + '&redirect=true'
              });

              res.writeHead(307, { location: redirectURL });
              res.end();
            } else {
              self._handler(null, path, operation, params, req, res, done);
            }
          }
        }
      });
    } else {
      this._handler(null, path, operation, params, req, res, done);
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
        schema = require(path.resolve(__dirname + '/../schemas/', operation + '.json'));
        return next(null, schema);
      } catch (err) {
        return next(err);
      }
    },

    function validateParams (schema, next) {
      var result = tv4.validateResult(params, schema);
      if (result.valid) {
        if (operation === 'create' || operation === 'append' || operation === 'delete' ||
            operation === 'setowner' || operation === 'setpermission' || operation === 'rename' ||
            operation === 'createsymlink' || operation === 'mkdirs') {
          if (self._opts.user && self._opts.user !== params['user.name']) {
            return next(new APIError('SecurityException', {
              message: 'user=' + params['user.name'] + ', access=WRITE'
            }));
          }
        }
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
