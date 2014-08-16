webhdfs-proxy
=============

**webhdfs-proxy** is a naive proxy layer for Hadoop WebHDFS REST API, which can be used to mock WebHDFS API requests in the tests or help to 
replace/migrate existing HDFS data storage (migration to S3, GridFS or custom storage etc.).

# Usage

Storage specific logic is implemented in storage middleware.
**webhdfs-proxy** itself implements basic requests validation, parsing and redirects simulation, and utilities for handling WehbHDFS API requests and responses.


Supported WebHDFS REST API operations (Hadoop 2.4.x compatible) at the moment are:

*  APPEND
*  CREATE
*  CREATESYMLINK
*  DELETE
*  GETFILESTATUS
*  LISTSTATUS
*  MKDIRS
*  OPEN
*  RENAME
*  SETOWNER
*  SETPERMISSIONS

Install module:

```bash
npm install webhdfs-proxy --save

```

Basic middleware skeleton:


```js
var WebHDFSProxy = require('webhdfs-proxy');

WebHDFSProxy.createServer({
  path: '/webhdfs/v1',
  validate: true,

  http: {
    port: 80
  },

  https: {
    port: 443,
    key: '/path/to/key',
    cert: '/path/to/cert'
  }
}, function storageHandler (err, path, operation, params, req, res, next) {
  // Pass error to WebHDFS REST API user
  if (err) {
    return next(err);
  }

  switch (operation) {
    case 'open':
      // Implement operation logic here
      return next();
      break;

    case 'create':
      // Implement operation logic here
      return next();
      break;
      
    case 'mkdirs':
      // Implement operation logic here
      return next(new Error('Internal error'));
      break;
  }
}, function onServerCreate (err, servers) {
  if (err) {
    console.log('WebHDFS proxy server was not created: ' + err.message);
    return;
  }

  // Proxy server was successfully created
});
```

For extended usage and implementing your own storage middleware, please see example middlewares.

## Server options

*  *path*  (optional, string, '/webhdfs/v1') - API endpoint path
*  *validate* (optional, boolean, 'true') - Enables requests validation. Supported schemas are loacated in *schemas/* directory.
*  *http* (optional, object) - HTTP options. HTTP server is created always.
*  *http.port* (optional, number, 80) - HTTP listening port.
*  *https* (optional, object) - HTTPS options.
*  *https.port* (optional, number, 443) - HTTPS listening port. If set then it enables HTTPS server automatically.
*  *https.key* (optional, string) - User defined path to key file. If not set then key will be generated on runtime.
*  *https.cert* (optional, string) - User defined path to cert file. If not set then certificate will be generated on runtime.

## Middleware parameters

*  *err* - It can be ignored or passed to the client. Is set if request validation failed or unexpected runtime error occurred. 
*  *path* - WebHDFS resource path
*  *operation* - WebHDFS API operation name
*  *params* - Map of parsed query parameters
*  *req* - Internal request object
*  *res* - Internal response object
*  *next* - Function which should be called when the request is fulfilled. Returns an error response if first argument is a valid error object.


# Tests

Running tests:

```bash
npm test

```

# Middlewares

Coming soon...

# Licence

MIT