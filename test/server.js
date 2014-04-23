'use strict';

var WebHDFSProxy = require('../');
var WebHDFS = require('webhdfs');

var fs = require('fs');
var path = require('path');
var must = require('must');
var demand = must;
var sinon = require('sinon');

var storage = {};

function handler (err, req, res, next) {
  console.log(req.url + '\n');

  // Forward error
  if (err) {
    return next(err);
  }

  switch (req.params.op) {
    case 'mkdirs':
      storage[req.path] = {
        accessTime: (new Date()).getTime(),
        blockSize: 0,
        group: 'supergroup',
        length: 24930,
        modificationTime: (new Date()).getTime(),
        owner: 'webuser',
        pathSuffix: '',
        permission: '644',
        replication: 1,
        type: 'DIRECTORY'
      };
      return next();
      break;

    case 'append':
    case 'create':
      var overwrite = true;
      var exists = storage.hasOwnProperty(req.path);
      var append = (req.params.op === 'append');

      if (req.params.hasOwnProperty('overwrite') && !req.params.overwrite) {
        overwrite = false;
      }

      if (!append && !overwrite && exists) {
        return next(new Error('File already exists'));
      }

      if (!exists) {
        storage[req.path] = {
          accessTime: (new Date()).getTime(),
          blockSize: 0,
          group: 'supergroup',
          length: 0,
          modificationTime: (new Date()).getTime(),
          owner: 'webuser',
          pathSuffix: '',
          permission: '644',
          replication: 1,
          type: 'FILE',
          data: ''
        };
      }

      req.on('data', function onData (data) {
        if (append || storage[req.path].data.length > 0) {
          storage[req.path].data += data.toString();
        } else {
          storage[req.path].data = data.toString();
        }
      });

      req.on('end', function onFinish () {
        storage[req.path].pathSuffix = path.basename(req.path);
        storage[req.path].length = storage[req.path].data.length;
        return next();
      });

      req.resume();
      break;

    case 'open':
      if (!storage.hasOwnProperty(req.path)) {
        return next(new Error('File doesn\'t exist'));
      }

      res.writeHead(200, {
        'content-length': storage[req.path].data.length,
        'content-type': 'application/octet-stream'
      });

      res.end(storage[req.path].data);
      return next();
      break;

    case 'liststatus':
      var files = [];
      for (var key in storage) {
        if (key !== req.path && path.dirname(key) === req.path) {
          files.push(storage[key]);
        }
      }

      var data = JSON.stringify({
        FileStatuses: {
          FileStatus: files
        }
      });

      res.writeHead(200, {
        'content-length': data.length,
        'content-type': 'application/json'
      });

      res.end(data);
      return next();
      break;

    case 'getfilestatus':
      if (!storage.hasOwnProperty(req.path)) {
        return next(new Error('File doesn\'t exist'));
      }

      var data = JSON.stringify({
        FileStatus: storage[req.path]
      });

      res.writeHead(200, {
        'content-length': data.length,
        'content-type': 'application/json'
      });

      res.end(data);
      return next();
      break;

    case 'rename':
      if (!storage.hasOwnProperty(req.path)) {
        return next(new Error('File doesn\'t exist'));
      }

      if (storage.hasOwnProperty(req.params.destination)) {
        return next(new Error('Destination path exist'));
      }

      storage[req.params.destination] = storage[req.path];
      delete storage[req.path];
      return next();
      break;

    case 'setpermission':
      if (!storage.hasOwnProperty(req.path)) {
        return next(new Error('File doesn\'t exist'));
      }

      storage[req.path].permission = req.params.permission;
      return next();
      break;

    case 'setowner':
      if (!storage.hasOwnProperty(req.path)) {
        return next(new Error('File doesn\'t exist'));
      }

      storage[req.path].owner = req.params.owner;
      storage[req.path].group = req.params.group;
      return next();
      break;

    default:
      return next();
      break;
  }
}

describe('WebHDFS Proxy', function () {
  // Setup WebHDFS client
  var proxyServer = null;
  var proxyClient = WebHDFS.createClient({
    user: 'webuser',
    host: 'localhost',
    port: 45000,
    path: '/webhdfs/v1'
  });

  // Set options
  var path = '/files/' + Math.random();
  var opts = {
    path: '/webhdfs/v1',
    http: {
      port: 45000
    }
  };

  before(function () {
    proxyServer = WebHDFSProxy.createServer(opts, handler);
  });

  it('should make a directory', function (done) {
    proxyClient.mkdir(path, function (err) {
      demand(err).be.null();
      return done();
    });
  });

  it('should create and write data to a file', function (done) {
    proxyClient.writeFile(path + '/file-1', 'random data', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should append content to an existing file', function (done) {
    proxyClient.appendFile(path + '/file-1', 'more random data', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should create and stream data to a file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = proxyClient.createWriteStream(path + '/file-2');
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      done();
    });
  });

  it('should append stream content to an existing file', function (done) {
    var localFileStream = fs.createReadStream(__filename);
    var remoteFileStream = proxyClient.createWriteStream(path + '/file-2', true);
    var spy = sinon.spy();

    localFileStream.pipe(remoteFileStream);
    remoteFileStream.on('error', spy);

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();

      done();
    });
  });

  it('should open and read a file stream', function (done) {
    var remoteFileStream = proxyClient.createReadStream(path + '/file-1');
    var spy = sinon.spy();
    var data = [];

    remoteFileStream.on('error', spy);
    remoteFileStream.on('data', function onData (chunk) {
      data.push(chunk);
    });

    remoteFileStream.on('finish', function () {
      demand(spy.called).be.falsy();
      demand(Buffer.concat(data).toString()).be.equal('random datamore random data');

      done();
    });
  });


  it('should open and read a file', function (done) {
    proxyClient.readFile(path + '/file-1', function (err, data) {
      demand(err).be.null();
      demand(data.toString()).be.equal('random datamore random data');
      done();
    });
  });

  it('should list directory status', function (done) {
    proxyClient.readdir(path, function (err, files) {
      demand(err).be.null();
      demand(files).have.length(2);

      demand(files[0].pathSuffix).to.eql('file-1');
      demand(files[1].pathSuffix).to.eql('file-2');

      demand(files[0].type).to.eql('FILE');
      demand(files[1].type).to.eql('FILE');
      done();
    });
  });

  it('should change file permissions', function (done) {
    proxyClient.chmod(path, '0777', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should change file owner', function (done) {
    proxyClient.chown(path, process.env.USER, 'supergroup', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should rename file', function (done) {
    proxyClient.rename(path + '/file-2', path + '/bigfile', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should return an error if destination file already exist', function (done) {
    proxyClient.rename(path + '/file-1', path + '/bigfile', function (err) {
      demand(err).be.not.null();
      done();
    });
  });

  it('should return an error if destination is missing', function (done) {
    proxyClient.rename(path + '/file-1', undefined, function (err) {
      demand(err).be.not.null();
      done();
    });
  });

  it('should check file existence', function (done) {
    proxyClient.exists(path + '/bigfile', function (exists) {
      demand(exists).be.true();

      done();
    });
  });

  it('should return false if file doesn\'t exist', function (done) {
    proxyClient.exists(path + '/bigfile2', function (exists) {
      demand(exists).be.falsy();

      done();
    });
  });

  it('should stat file', function (done) {
    proxyClient.stat(path + '/bigfile', function (err, stats) {
      demand(err).be.null();
      demand(stats).be.object();

      demand(stats.type).to.eql('FILE');
      demand(stats.owner).to.eql('webuser');

      done();
    });
  });

  it('should return an error if trying to stat unexisting file', function (done) {
    proxyClient.stat(path + '/bigfile2', function (err, stats) {
      demand(err).be.not.null();

      done();
    });
  });

  /*
  it('should create symbolic link', function (done) {
    hdfs.symlink(path+ '/bigfile', path + '/biggerfile', function (err) {
      // Pass if server doesn't support symlinks
      if (err.message.indexOf('Symlinks not supported') !== -1) {
        done();
      } else {
        demand(err).be.null();
        done();
      }
    });
  });

  it('should delete file', function (done) {
    hdfs.rmdir(path+ '/file-1', function (err) {
      demand(err).be.null();
      done();
    });
  });

  it('should delete directory recursively', function (done) {
    hdfs.rmdir(path, true, function (err) {
      demand(err).be.null();
      done();
    });
  });
  */
});