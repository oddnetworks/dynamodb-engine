/* global describe, beforeAll, it, expect */
/* eslint max-lines: 0 */
'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var U = require('../lib/utils');
var lib = require('./support/lib');

var Map = Immutable.Map;

describe('API relationships', function () {
	var SUBJECT = new Map({
		seriesLimit: 300,
		characterLimit: 200,
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
			SCHEMA: this.SCHEMA,
			DEFAULT_THROUGHPUT: {read: 50, write: 50}
		};

		Promise.resolve(args)
			// Setup a fresh database.
			.then(lib.initializeDb)
			.then(function (newArgs) {
				args = newArgs;
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

			// Create relationships for the records
			.then(function () {
				var singleSeries = SUBJECT.get('series').toJS()[0];
				return Promise.all(SUBJECT.get('characters').toJS().map(U.partial(
					createRelation,
					args.db,
					singleSeries
				)));
			})

			// Create reverse relationships for the records
			.then(function () {
				var character = SUBJECT.get('characters').toJS()[0];
				return Promise.all(SUBJECT.get('series').toJS().map(function (series) {
					return createRelation(args.db, series, character);
				}));
			})

			// Fetch relationship links
			.then(function () {
				var singleSeries = SUBJECT.get('series').toJS()[0];
				return args.db.getRelations(singleSeries.id, 'Character');
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

			// Fetch reverse relationship links
			.then(function () {
				var character = SUBJECT.get('characters').toJS()[0];
				return args.db.getReverseRelations(character.id, 'Series');
			})
			.then(function (res) {
				SUBJECT = SUBJECT.set('reverseLinks', Immutable.fromJS(res));
				return res;
			})

			// Fetch reverse relationship records
			.then(function (links) {
				return Promise.all(links.map(function (link) {
					return args.db.getRecord(link.type, link.id);
				}));
			})
			.then(function (records) {
				SUBJECT = SUBJECT.set('reverseRecords', Immutable.fromJS(records));
			})

			// Remove a relation
			.then(function () {
				var series = SUBJECT.get('series').toJS()[0];
				var character = SUBJECT.get('characters').toJS()[0];
				return args.db.removeRelation(series.id, character.id);
			})

			// Fetch relationship links
			.then(function () {
				var singleSeries = SUBJECT.get('series').toJS()[0];
				return args.db.getRelations(singleSeries.id, 'Character');
			})
			.then(function (res) {
				SUBJECT = SUBJECT.set('linksAfterRm', Immutable.fromJS(res));
				return res;
			})

			// Fetch reverse relationship links
			.then(function () {
				var character = SUBJECT.get('characters').toJS()[0];
				return args.db.getReverseRelations(character.id, 'Series');
			})
			.then(function (res) {
				SUBJECT = SUBJECT.set('reverseLinksAfterRm', Immutable.fromJS(res));
				return res;
			})

			// Finis
			.then(done)
			.catch(done.fail);
	}, 300 * 1000); // Allow to run for a long time

	it('fetches correct number of related records', function () {
		var relatedRecords = SUBJECT.get('relatedRecords');
		expect(relatedRecords.size).toBe(SUBJECT.get('characterLimit'));
	});

	it('returns valid records as related records', function () {
		var record = U.sample(SUBJECT.get('relatedRecords').toJS());
		expect(record.id).toBeTruthy();
		expect(record.type).toBe('Character');
		expect(record.name).toBeTruthy();
		expect(record.series).toBeTruthy();
	});

	it('fetches correct number of reverse relations', function () {
		var relatedRecords = SUBJECT.get('reverseRecords');
		expect(relatedRecords.size).toBe(SUBJECT.get('seriesLimit'));
	});

	it('returns valid records as related records', function () {
		var record = U.sample(SUBJECT.get('reverseRecords').toJS());
		expect(record.id).toBeTruthy();
		expect(record.type).toBe('Series');
		expect(record.title).toBeTruthy();
		expect(record.creators).toBeTruthy();
	});

	it('fetches correct number of relations after removal', function () {
		var related = SUBJECT.get('linksAfterRm');
		var reverse = SUBJECT.get('reverseLinksAfterRm');
		expect(related.size).toBe(SUBJECT.get('characterLimit') - 1);
		expect(reverse.size).toBe(SUBJECT.get('seriesLimit') - 1);
	});
});

function createRelation(db, subject, character) {
	return db.createRelation(subject, character);
}

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
