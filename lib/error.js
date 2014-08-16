'use strict';

/**
 * @module APIError
 *
 * Maps RemoteException errors defined in
 * http://hadoop.apache.org/docs/r2.2.0/hadoop-project-dist/hadoop-hdfs/WebHDFS.html#Error_Responses
 */

var errors = {
  IllegalArgumentException: {
    statusCode: 400,
    javaClassName: 'java.lang.IllegalArgumentException',
    message: '%message%'
  },

  UnsupportedOperationException: {
    statusCode: 400,
    javaClassName: 'java.lang.UnsupportedOperationException',
    message: 'op=%operation% is not supported'
  },

  SecurityException: {
    statusCode: 401,
    javaClassName: 'org.apache.hadoop.security.AccessControlException',
    message: 'Permission denied: %message%'
  },

  AccessControlException: {
    statusCode: 403,
    javaClassName: 'java.lang.SecurityException',
    message: 'Failed to obtain user group information: %message%'
  },

  FileNotFoundException: {
    statusCode: 404,
    javaClassName: 'java.io.FileNotFoundException',
    message: 'File does not exist: %path%'
  },

  RuntimeException: {
    statusCode: 500,
    javaClassName: 'java.lang.RuntimeException',
    message: '%message%'
  }
};

/**
 *
 * @param exception
 * @param data
 * @constructor
 */
function APIError (exception, data) {
  Error.call(this);
  Error.captureStackTrace(this, this.constructor);

  this.name = this.constructor.name;

  // Find defined exception
  if (errors.hasOwnProperty(exception)) {
    this.exception = exception;
    this.statusCode = errors[exception].statusCode;
    this.javaClassName = errors[exception].javaClassName;
    this.message = errors[exception].message;

    // Replace placeholders
    for (var key in data) {
      this.message = this.message.replace('%' + key + '%', data[key]);
    }
  }
}

APIError.prototype = Object.create(Error.prototype);

APIError.prototype.toJSON = function toJSON () {
  return JSON.stringify({
    RemoteException: {
      exception: this.exception,
      javaClassName: this.javaClassName,
      message: this.message
    }
  });
};

module.exports.ERRORS = errors;
module.exports.APIError = APIError;