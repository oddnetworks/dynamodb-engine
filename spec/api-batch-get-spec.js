/* global describe, beforeAll, it, expect */
/* eslint max-lines: 0 */
'use strict';
var Promise = require('bluebird');
var U = require('../lib/utils');
var lib = require('./support/lib');

describe('API batchGet()', function () {
	var TYPE = 'Character';
	var args;
	var docs;
	var existing;
	var missing;
	var keys;
	var results;
	var testServer;

	beforeAll(function (done) {
		args = {
			AWS_ACCESS_KEY_ID: this.AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY: this.AWS_SECRET_ACCESS_KEY,
			AWS_REGION: this.AWS_REGION,
			DYNAMODB_ENDPOINT: this.DYNAMODB_ENDPOINT,
			TABLE_PREFIX: this.TABLE_PREFIX,
			SCHEMA: this.SCHEMA,
			DEFAULT_THROUGHPUT: {read: 50, write: 50}
		};

		return Promise.resolve(args)
			// Setup a fresh database.
			.then(() => {
				return lib.startTestServer().then(server => {
					testServer = server;
					return args;
				});
			})
			.then(lib.initializeDb)
			.then(function (newArgs) {
				args = newArgs;
				return args;
			})

			// Load the fixtures from files
			.then(function () {
				return loadFixtures(50).then(res => {
					docs = res;
					return null;
				});
			})

			// Insert the new records
			.then(function () {
				return Promise.all(docs.map(U.partial(lib.putItem, args)));
			})

			// Compose fetch keys
			.then(function () {
				existing = U.sampleSize(docs, 20).map(function (item) {
					return item.id;
				});

				missing = U.range(5).map(function (i) {
					return i + '-foo-bar';
				});

				keys = U.shuffle(existing.concat(missing));
			})

			// batchGet items
			.then(function () {
				return args.db.batchGet(TYPE, keys).then(function (res) {
					results = res;
					return null;
				});
			})

			.then(done)
			.catch(done.fail);
	});

	afterAll(function (done) {
		testServer.close(function () {
			done();
		});
	});

	it('returns all items that could be found', function () {
		expect(results.length).toBe(20);

		// A returned record has the expected attributes
		var rec = U.sample(results);
		expect(rec.type).toBe(TYPE);
		expect(U.isString(rec.name)).toBe(true);
		expect(U.isString(rec.thumbnail.url)).toBe(true);
		expect(Array.isArray(rec.series)).toBe(true);
	});

	it('returns ALL found items', function () {
		expect(results.length).toBe(20);

		const found = results.map(function (item) {
			return item.id;
		});

		existing.forEach(function (id) {
			expect(found).toContain(id);
		});
	});

	it('does not return non-existing items', function () {
		expect(results.length).toBe(20);

		const found = results.map(function (item) {
			return item.id;
		});

		missing.forEach(function (id) {
			expect(found).not.toContain(id);
		});
	});
});

function loadFixtures(limit) {
	var count = 0;

	var filePaths = lib.listFixturePaths(function (pathPart) {
		if (count < limit && pathPart.indexOf('characters') >= 0) {
			count += 1;
			return true;
		}
		return false;
	});

	return Promise.all(filePaths.map(function (path) {
		return lib.readJsonFile(path).then(function (data) {
			return lib.mapFixtureToRecord(data);
		});
	}));
}
