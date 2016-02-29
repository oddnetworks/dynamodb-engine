/* global describe, beforeAll, it, expect */
'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var Map = Immutable.Map;

describe('API relationships', function () {
	var subject = new Map({
		limit: 200
	});
	var constants;
	var lib;
	var U;

	beforeAll(function (done) {
		constants = this.constants;
		lib = this.lib;
		U = this.U;

		var args = new Map(constants);
		args = args.set('lib', lib);
		args = args.set('U', U);

		Promise.resolve(args)
			// Setup a fresh database.
			.then(lib.initializeDb)
			.then(function (newArgs) {
				args = newArgs;
				return args;
			})

			// Load the fixtures from files
			.then(U.partial(loadFixtures, subject.get('limit')))
			.then(function (docs) {
				subject = subject.set('series', docs.filter(filterBySeries)[0]);
				subject = subject.set('characters', docs.filter(filterByCharacter));
				return docs;
			})

			// Insert the new records
			.then(function (docs) {
				return Promise.all(docs.map(U.partial(lib.putItem, args)));
			})

			// Create relationships for the records
			.then(function () {
				return Promise.all(subject.get('characters').map(U.partial(
					createRelation,
					args.get('db'),
					subject.get('series')
				)));
			})

			// Fetch relationship links
			.then(function () {
				var db = args.get('db');
				var series = subject.get('series');
				return db.getRelations(series.id, 'Character');
			})
			.then(function (res) {
				subject = subject.set('links', res);
				return res;
			})

			// Fetch relationship records
			.then(function (links) {
				var db = args.get('db');
				return Promise.all(links.map(function (link) {
					return db.getRecord(link.type, link.id);
				}));
			})
			.then(function (records) {
				subject = subject.set('relatedRecords', records);
			})

			// Finis
			.then(done, done.fail);
	}, 300 * 1000); // Allow to run for a long time

	it('fetches correct number of related records', function () {
		var relatedRecords = subject.get('relatedRecords');
		expect(relatedRecords.length).toBe(200);
	});

	it('returns valid records as related records', function () {
		var record = U.sample(subject.get('relatedRecords'));
		expect(record.id).toBeTruthy();
		expect(record.type).toBe('Character');
		expect(record.name).toBeTruthy();
		expect(record.series).toBeTruthy();
	});
});

function createRelation(db, subject, character) {
	return db.createRelation(subject.id, character);
}

function filterByCharacter(item) {
	return item.type === 'Character';
}

function filterBySeries(item) {
	return item.type === 'Series';
}

function loadFixtures(characterLimit, args) {
	var lib = args.get('lib');
	var series;
	var characterCount = 0;

	var filePaths = lib.listFixturePaths(function (pathPart) {
		if (!series && pathPart.indexOf('series') >= 0) {
			series = 1;
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
