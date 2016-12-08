'use strict';

process.env.NODE_TLS_REJECT_UNAUTHORIZED = 0;

var WebHDFSProxy = require('../');
var WebHDFS = require('webhdfs');

var fs = require('fs');
var p = require('path');
var demand = require('must');
var sinon = require('sinon');

var storage = {};

/**
 *
 * @param err
 * @param path
 * @param operation
 * @param params
 * @param req
 * @param res
 * @param next
 * @returns {*}
 */
function testHandler (err, path, operation, params, req, res, next) {
  // Forward error
  if (err) {
    return next(err);
  }

  switch (operation) {
    case 'mkdirs':
      if (!parseInt(params.permissions)) {
        return next(new Error('Invalid permission value'));
      }
      break;
  }

  return next();
}

var handler = sinon.spy(testHandler);

describe('WebHDFS Proxy', function () {
  // Setup WebHDFS client
  var proxyServer = null;

  // Set options
  var path = null;
  var opts = {
    path: '/webhdfs/v1',
    user: 'webuser',
    http: {
      port: 45000
    },
    https: {
      port: 46000
    }
  };

  before(function (done) {
    proxyServer = WebHDFSProxy.createServer(opts, handler, done);
  });

  function createTests (title, client) {
    describe(title, function () {
      before(function () {
        path = '/files/' + Math.random();
      });

      it('should succeed if supported operation was executed', function (done) {
        client.mkdir(path, function (err) {
          demand(err).be.null();
          demand(handler.calledWithMatch(null, path, 'mkdirs', { op: 'mkdirs', 'user.name': 'webuser', permissions: '0777' })).be.truthy();

          return done();
        });
      });

      it('should succeed if supported write operation with wrong user returned an error', function (done) {
        client._opts.user = 'wronguser';
        client.mkdir(path, function (err) {
          demand(err).not.be.null();
          client._opts.user = 'webuser';
          return done();
        });
      });

      it('should succeed if supported operation returned an error', function (done) {
        client.mkdir(path, 'invalid', function (err) {
          demand(err).not.be.null();

          return done();
        });
      });

      it('should succeed if unsupported operation request returned an error', function (done) {
        var url = client._getOperationEndpoint('randomoperation', path, {});
        client._sendRequest('PUT', url, function onResponse (err) {
          demand(err).not.be.null();
          demand(err.message).eql('op=randomoperation is not supported');

          return done();
        });
      });

      it('should succeed if operation request with invalid method returned an error', function (done) {
        var url = client._getOperationEndpoint('mkdirs', path, {});
        client._sendRequest('GET', url, function onResponse (err) {
          demand(err).not.be.null();
          demand(err.message).eql('Invalid HTTP method GET, expected PUT');

          return done();
        });
      });

      it('should succeed if operation request body validation fails', function (done) {
        var url = client._getOperationEndpoint('rename', path, {});
        client._sendRequest('PUT', url, function onResponse (err) {
          demand(err).not.be.null();
          demand(err.message).eql('Missing required property: destination');

          return done();
        });
      });

      it('should succeed if operation request returns redirect', function (done) {
        var url = client._getOperationEndpoint('create', path, { });
        client._sendRequest('PUT', url, function onResponse (err, res) {
          demand(res.headers).own('location');
          demand(res.statusCode).eql(307);

          return done();
        });
      });
    });
  }

  createTests('HTTP', WebHDFS.createClient({
    user: 'webuser',
    host: 'localhost',
    port: 45000,
    path: '/webhdfs/v1'
  }));

  createTests('HTTPS', WebHDFS.createClient({
    user: 'webuser',
    host: 'localhost',
    port: 46000,
    path: '/webhdfs/v1',
    protocol: 'https'
  }));
});