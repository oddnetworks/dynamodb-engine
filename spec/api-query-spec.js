/* global describe, beforeAll, afterAll, it, expect */
/* eslint max-lines: 0 */
/* eslint max-nested-callbacks: 0 */
'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var U = require('../lib/utils');
var lib = require('./support/lib');

var Map = Immutable.Map;

describe('API .query', function () {
	var db = null;

	var SUBJECT = new Map({
		seriesLimit: 300,
		characterLimit: 200,
		series: null,
		characters: null
	});

	var testServer;

	beforeAll(function (done) {
		var args = {
			AWS_ACCESS_KEY_ID: this.AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY: this.AWS_SECRET_ACCESS_KEY,
			AWS_REGION: this.AWS_REGION,
			DYNAMODB_ENDPOINT: this.DYNAMODB_ENDPOINT,
			TABLE_PREFIX: this.TABLE_PREFIX,
			SCHEMA: this.SCHEMA,
			DEFAULT_THROUGHPUT: {read: 50, write: 50}
		};

		Promise.resolve(args)
			// Setup a new server instance.
			.then(function () {
				return lib.startTestServer().then(server => {
					testServer = server;
					return args;
				});
			})
			// Setup a fresh database.
			.then(lib.initializeDb)
			.then(function (newArgs) {
				args = newArgs;
				db = args.db;
				return args;
			})

			// Load the fixtures from files
			.then(U.partial(loadFixtures, SUBJECT.get('characterLimit'), SUBJECT.get('seriesLimit')))
			.then(function (docs) {
				SUBJECT = SUBJECT.set('series', Immutable.fromJS(docs.filter(filterBySeries)));
				SUBJECT = SUBJECT.set('characters', Immutable.fromJS(docs.filter(filterByCharacter)));
				return docs;
			})

			// Insert the new records
			.then(function (docs) {
				return Promise.all(docs.map(U.partial(lib.putItem, args)));
			})

			// Finis
			.then(done)
			.catch(done.fail);
	}, 300 * 1000); // Allow to run for a long time

	afterAll(function (done) {
		testServer.close(function () {
			done();
		});
	});

	describe('fetchAll()', function () {
		var query = null;
		beforeAll(function () {
			query = db.query('Character', 'ByName').hashEqual('Character');
		});

		describe('defaults', function () {
			var results = null;

			beforeAll(function (done) {
				return query.fetchAll()
					.then(res => {
						results = res;
					})
					.then(done)
					.catch(done.fail);
			});

			it('has results', function () {
				expect(results.length).toBe(SUBJECT.get('characterLimit'));
			});
		});
	});

	describe('fetchPage()', function () {
		var query = null;
		beforeAll(function () {
			query = db.query('Character', 'ByName').hashEqual('Character');
		});

		describe('defaults', function () {
			var result = null;

			beforeAll(function (done) {
				return query.fetchPage()
					.then(res => {
						result = res;
					})
					.then(done)
					.catch(done.fail);
			});

			it('has result.count', function () {
				expect(result.count).toBe(SUBJECT.get('characterLimit'));
			});

			it('has result.items', function () {
				expect(result.items.length).toBe(SUBJECT.get('characterLimit'));
			});

			it('has Character items', function () {
				var record = U.sample(result.items);
				expect(record.type).toBe('Character');
				expect(typeof record.name).toBe('string');
				expect(Array.isArray(record.series)).toBe(true);
			});
		});

		describe('with limit', function () {
			var result = null;

			beforeAll(function (done) {
				return query
					.setLimit(10)
					.fetchPage()
					.then(res => {
						result = res;
					})
					.then(done)
					.catch(done.fail);
			});

			it('has result.count', function () {
				expect(result.count).toBe(10);
			});

			it('sorts ascending by default', function () {
				let first = U.first(result.items);
				let last = U.last(result.items);
				expect(first.name).toBe('Absorbing Man');
				expect(last.name).toBe('Apocalypse');
			});

			it('has lastEvaluatedKey', function () {
				let key = result.lastEvaluatedKey;
				expect(key.id).toBe('1009156');
				expect(key.type).toBe('Character');
				expect(key.name).toBe('Apocalypse');
			});
		});

		describe('with descending', function () {
			var result = null;

			beforeAll(function (done) {
				return query
					.descending()
					.setLimit(10)
					.fetchPage()
					.then(res => {
						result = res;
					})
					.then(done)
					.catch(done.fail);
			});

			it('has result.count', function () {
				expect(result.count).toBe(10);
			});

			it('sorts ascending by default', function () {
				let first = U.first(result.items);
				let last = U.last(result.items);
				expect(first.name).toBe('Spider-Girl (Anya Corazon)');
				expect(last.name).toBe('Lila Cheney');
			});
		});

		describe('by paging', function () {
			var pages = [];
			var pageLimit = 25;

			beforeAll(function (done) {
				var q = query.setLimit(pageLimit);

				function nextPage(lastEvaluatedKey) {
					return q.fetchPage(lastEvaluatedKey).then(page => {
						pages.push(page);
						if (page.lastEvaluatedKey) {
							return nextPage(page.lastEvaluatedKey);
						}
						return done();
					}).catch(done.fail);
				}

				nextPage();
			});

			it('got all pages', function () {
				var expectedLength = (SUBJECT.get('characterLimit') / pageLimit) + 1;
				expect(expectedLength).toBeTruthy();
				expect(pages.length).toBe(expectedLength);
			});

			it('has empty last page', function () {
				var last = U.last(pages);
				expect(last.items.length).toBe(0);
			});
		});
	});
});

function filterByCharacter(item) {
	return item.type === 'Character';
}

function filterBySeries(item) {
	return item.type === 'Series';
}

function loadFixtures(characterLimit, seriesLimit) {
	var seriesCount = 0;
	var characterCount = 0;

	var filePaths = lib.listFixturePaths(function (pathPart) {
		if (seriesCount < seriesLimit && pathPart.indexOf('series') >= 0) {
			seriesCount += 1;
			return true;
		}
		if (characterCount < characterLimit && pathPart.indexOf('characters') >= 0) {
			characterCount += 1;
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
