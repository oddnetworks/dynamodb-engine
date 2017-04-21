/* global describe, beforeAll, afterAll, it, expect */
/* eslint-disable max-nested-callbacks */
/* eslint max-lines: 0 */

'use strict';

var Promise = require('bluebird');
var Immutable = require('immutable');

var U = require('../lib/utils');
var lib = require('./support/lib');

describe('API CRUD operations', function () {
	var DB;
	var ARGS;
	var testServer;

	beforeAll(function (done) {
		ARGS = {
			AWS_ACCESS_KEY_ID: this.AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY: this.AWS_SECRET_ACCESS_KEY,
			AWS_REGION: this.AWS_REGION,
			DYNAMODB_ENDPOINT: this.DYNAMODB_ENDPOINT,
			TABLE_PREFIX: this.TABLE_PREFIX,
			SCHEMA: this.SCHEMA
		};

		Promise.resolve(ARGS)
			// Setup a fresh database.
			.then(function () {
				return lib.startTestServer().then(server => {
					testServer = server;
					return ARGS;
				});
			})
			.then(lib.initializeDb)
			.then(function (newArgs) {
				ARGS = newArgs;
				DB = ARGS.db;
				return null;
			})
			.then(done)
			.catch(done.fail);
	}, 50000);

	afterAll(function (done) {
		testServer.close(function () {
			done();
		});
	});

	describe('CRUD with valid data', function () {
		var SUBJECT = new Immutable.Map({
			createResult: null,
			firstReadResult: null,
			updateResult: null,
			secondReadResult: null,
			deleteResult: null,
			thirdReadResult: null
		});

		var DOC = U.deepFreeze(lib.createSeries({
			id: U.uniqueId('series-'),
			title: 'Bat Man',
			description: 'Batman and Gotham',
			resourceURI: 'http://example.com/batman-and-gotham',
			urls: ['http://example.com/series/batman-and-gotham']
		}));

		beforeAll(function (done) {
			Promise.resolve(DB)
				.then(function (DB) {
					return DB.createRecord(DOC);
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('createResult', Immutable.fromJS(res));
				})
				.then(function () {
					var doc = SUBJECT.get('createResult');
					return DB.getRecord(doc.get('type'), doc.get('id'));
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('firstReadResult', Immutable.fromJS(res));
				})
				.then(function () {
					var doc = SUBJECT.get('createResult').toJS();
					doc.creators.author = 'Jethro';
					return DB.updateRecord(doc);
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('updateResult', Immutable.fromJS(res));
				})
				.then(function () {
					var doc = SUBJECT.get('createResult');
					return DB.getRecord(doc.get('type'), doc.get('id'));
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('secondReadResult', Immutable.fromJS(res));
				})
				.then(function () {
					var doc = SUBJECT.get('createResult');
					return DB.removeRecord(doc.get('type'), doc.get('id'));
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('deleteResult', Immutable.fromJS(res));
				})
				.then(function () {
					var doc = SUBJECT.get('createResult');
					return DB.getRecord(doc.get('type'), doc.get('id'))
						.catch(DB.NotFoundError, function (err) {
							return err;
						});
				})
				.then(function (res) {
					SUBJECT = SUBJECT.set('thirdReadResult', res);
				})
				.then(done)
				.catch(done.fail);
		});

		it('creates a new record', function () {
			var doc = SUBJECT.get('createResult').toJS();
			expect(doc.id).toMatch(/^series-/);
			expect(doc.type).toBe('Series');
			expect(doc.title).toBe('Bat Man');
			expect(doc.urls).toEqual(['http://example.com/series/batman-and-gotham']);
			expect(doc.creators).toEqual({});
		});

		it('reads a new record', function () {
			var doc = SUBJECT.get('firstReadResult').toJS();
			expect(doc.id).toMatch(/^series-/);
			expect(doc.type).toBe('Series');
			expect(doc.title).toBe('Bat Man');
			expect(doc.urls).toEqual(['http://example.com/series/batman-and-gotham']);
			expect(doc.creators).toEqual({});
		});

		it('updates a record', function () {
			var doc = SUBJECT.get('updateResult').toJS();
			expect(doc.id).toMatch(/^series-/);
			expect(doc.type).toBe('Series');
			expect(doc.title).toBe('Bat Man');
			expect(doc.urls).toEqual(['http://example.com/series/batman-and-gotham']);
			expect(doc.creators).toEqual({author: 'Jethro'});
		});

		it('reads an updated record', function () {
			var doc = SUBJECT.get('secondReadResult').toJS();
			expect(doc.id).toMatch(/^series-/);
			expect(doc.type).toBe('Series');
			expect(doc.title).toBe('Bat Man');
			expect(doc.urls).toEqual(['http://example.com/series/batman-and-gotham']);
			expect(doc.creators).toEqual({author: 'Jethro'});
		});

		it('deletes a record', function () {
			expect(SUBJECT.get('deleteResult')).toBe(true);
		});

		it('cannot read a deleted record', function () {
			var err = SUBJECT.get('thirdReadResult');
			expect(err.name).toBe('NotFoundError');
			expect(err.message).toBe('Could not find record');
		});
	});
});
