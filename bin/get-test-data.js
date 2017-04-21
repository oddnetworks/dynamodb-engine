#!/usr/bin/env node
'use strict';
const HTTP = require('http');
const QS = require('querystring');
const CRYPTO = require('crypto');
const UTIL = require('util');
const Readable = require('stream').Readable;
const Writable = require('stream').Writable;
const Promise = require('bluebird');
const FilePath = require('filepath');

const HOSTNAME = 'gateway.marvel.com';
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const PUBLIC_KEY = process.env.PUBLIC_KEY;

if (!PUBLIC_KEY) {
	console.error('missing PUBLIC_KEY');
	process.exit(1);
}
if (!PRIVATE_KEY) {
	console.error('missing PRIVATE_KEY');
	process.exit(1);
}

// The strategy is to create a stream of Marvel JSON documents from the
// Marvel API and pipe it into a writable stream which will write them to
// disk as JSON files.

function ItemStream(spec) {
	Readable.call(this, {
		objectMode: true
	});
	this._pageIndex = 0;
	this.path = spec.path;
	this._isReading = false;
}

UTIL.inherits(ItemStream, Readable);

ItemStream.prototype._read = function () {
	if (!this._isReading) {
		this._doRead();
	}
};

ItemStream.prototype._doRead = function () {
	this._isReading = true;
	ItemStream
		.getCharactersPage(this.path, this._pageIndex)
		.then(res => {
			if (res.length === 0) {
				return this.push(null);
			}
			res.forEach(this.push.bind(this));
			this._pageIndex += 1;
			this._doRead();
		})
		.catch(err => {
			this.emit('error', err);
		});
};

// Get pages indexed at 0
ItemStream.getCharactersPage = function (path, n) {
	return makeRequest({
		path: path,
		query: {
			limit: 20,
			offset: 20 * n
		}
	})
	.then(function (res) {
		if (res.data && res.data.results) {
			return res.data.results;
		}
		return Promise.reject(new Error('Unexpected API response.'));
	});
};

// spec.directory
function FileStream(spec) {
	Writable.call(this, {
		objectMode: true
	});

	this.directory = FilePath.create(spec.directory);
}

UTIL.inherits(FileStream, Writable);

FileStream.prototype._write = function (chunk, encoding, callback) {
	this.directory
		.append(chunk.id + '.json')
		.write(JSON.stringify(chunk, null, 2))
		.catch(callback)
		.then(() => {
			callback();
			this.emit('item', chunk.id);
		});
};

// Make an HTTP request to the Marvel API
// Params:
//   args.path
//   args.query
function makeRequest(args) {
	const ts = new Date().getTime().toString();

	const hash = CRYPTO.createHash('md5');
	hash.update(ts + PRIVATE_KEY + PUBLIC_KEY);

	const auth = {
		ts: ts,
		apikey: PUBLIC_KEY,
		hash: hash.digest('hex')
	};

	const query = Object.keys(auth).reduce(function (query, key) {
		query[key] = auth[key];
		return query;
	}, args.query || {});

	const path = args.path + '?' + QS.stringify(query);

	return new Promise(function (resolve, reject) {
		const params = {
			hostname: HOSTNAME,
			method: 'GET',
			path: path,
			headers: {
				Accept: 'application/json'
			}
		};

		function returnResponse(status, data) {
			if (status !== 200) {
				return reject(data);
			}
			resolve(data);
		}

		const req = HTTP.request(params, function (res) {
			const contentLength = res.headers['Content-Length'] || res.headers['content-length'];
			if (parseInt(contentLength, 10)) {
				let json = '';
				res.setEncoding('utf8');
				res.on('data', function (chunk) {
					json += chunk;
				});
				res.on('end', function () {
					returnResponse(res.statusCode, JSON.parse(json));
				});
			} else {
				returnResponse(res.statusCode, null);
			}
		});

		req.on('error', reject);
		req.end();
	});
}

function getItems(datatype) {
	console.log('getting %s', datatype);

	let readStream = new ItemStream({
		path: '/v1/public/' + datatype
	});
	let writeStream = new FileStream({
		directory: FilePath.create(__dirname)
			.dir()
			.append('test', 'fixtures', 'Marvel', datatype)
	});

	writeStream.on('item', function () {
		process.stdout.write('.');
	});
	writeStream.on('finish', function () {
		process.stdout.write('\n');
		console.log('%s data written in %s', datatype, writeStream.directory);
	});

	// All we need to do is hook our streams together with .pipe() and
	// everything starts running on its own.
	return readStream.pipe(writeStream);
}

// Execute the script if we're not loaded as a module.
if (require.main === module) {
	getItems('characters');
	getItems('series');
}
