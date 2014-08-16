'use strict';

var WebHDFSProxyServer = require('./lib/server');

/**
 * Module exports
 */
module.exports = {
  /**
   * Create a new proxy server instance and start accepting requests
   *
   * @param {Object} opts Server options
   * @param {Function} done Called when server(s) initialization is done
   * @param {WebHDFSProxyServer~MiddlewareHandler} middleware Middleware handler
   */
  createServer: function createServer (opts, middleware, done) {
    return new WebHDFSProxyServer(opts, middleware, done);
  },

  /**
   * Expose error class
   */
  WebHDFSAPIError: require('./lib/error').APIError,
  WebHDFSAPIErrors: require('./lib/error').ERRORS
};
