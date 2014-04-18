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
   * @param {WebHDFSProxyServer~MiddlewareHandler} middleware Middleware handler
   */
  createServer: function createServer (opts, middleware) {
    return new WebHDFSProxyServer(opts, middleware);
  }
};
