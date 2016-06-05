/* global describe, beforeAll, it, expect */
'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var Map = Immutable.Map;

var U = require('../lib/utils');
var lib = require('./support/lib');

describe('API relationships', function () {
	var SUBJECT = new Map({
		limit: 200,
		series: null,
		characters: null,
		links: null,
		relatedRecords: null
	});

	beforeAll(function (done) {
		var args = {
			AWS_ACCESS_KEY_ID: this.AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY: this.AWS_SECRET_ACCESS_KEY,
			AWS_REGION: this.AWS_REGION,
			DYNAMODB_ENDPOINT: this.DYNAMODB_ENDPOINT,
			TABLE_PREFIX: this.TABLE_PREFIX,
			SCHEMA: this.SCHEMA
		};

		Promise.resolve(args)
			// Setup a fresh database.
			.then(lib.initializeDb)
			.then(function (newArgs) {
				args = newArgs;
				return args;
			})

			// Load the fixtures from files
			.then(U.partial(loadFixtures, SUBJECT.get('limit')))
			.then(function (docs) {
				SUBJECT = SUBJECT.set('series', Immutable.fromJS(docs.filter(filterBySeries)[0]));
				SUBJECT = SUBJECT.set('characters', Immutable.fromJS(docs.filter(filterByCharacter)));
				return docs;
			})

			// Insert the new records
			.then(function (docs) {
				return Promise.all(docs.map(U.partial(lib.putItem, args)));
			})

			// Create relationships for the records
			.then(function () {
				return Promise.all(SUBJECT.get('characters').toJS().map(U.partial(
					createRelation,
					args.db,
					SUBJECT.get('series').toJS()
				)));
			})

			// Fetch relationship links
			.then(function () {
				var series = SUBJECT.get('series').toJS();
				return args.db.getRelations(series.id, 'Character');
			})
			.then(function (res) {
				SUBJECT = SUBJECT.set('links', Immutable.fromJS(res));
				return res;
			})

			// Fetch relationship records
			.then(function (links) {
				return Promise.all(links.map(function (link) {
					return args.db.getRecord(link.type, link.id);
				}));
			})
			.then(function (records) {
				SUBJECT = SUBJECT.set('relatedRecords', Immutable.fromJS(records));
			})

			// Finis
			.then(done)
			.catch(done.fail);
	}, 300 * 1000); // Allow to run for a long time

	it('fetches correct number of related records', function () {
		var relatedRecords = SUBJECT.get('relatedRecords');
		expect(relatedRecords.size).toBe(SUBJECT.get('limit'));
	});

	it('returns valid records as related records', function () {
		var record = U.sample(SUBJECT.get('relatedRecords').toJS());
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

function loadFixtures(characterLimit) {
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
