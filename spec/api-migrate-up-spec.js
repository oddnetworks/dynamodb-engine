/* global describe, beforeAll, afterAll, it, expect */
/* eslint max-lines: 0 */

'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var U = require('../lib/utils');
var lib = require('./support/lib');

var Map = Immutable.Map;

describe('API migrateUp()', function () {
	var SUBJECT = new Map({
		initialTableList: null,
		tables: null,
		withIndexes: null
	});

	var testServer;

	beforeAll(function (done) {
		var args = {
			AWS_ACCESS_KEY_ID: this.AWS_ACCESS_KEY_ID,
			AWS_SECRET_ACCESS_KEY: this.AWS_SECRET_ACCESS_KEY,
			AWS_REGION: this.AWS_REGION,
			DYNAMODB_ENDPOINT: this.DYNAMODB_ENDPOINT,
			TABLE_PREFIX: this.TABLE_PREFIX
		};

		args.db = lib.createDbInstance(args, {Character: {}, Series: {}});

		Promise.resolve(args)
			// Setup a new server instance.
			.then(function () {
				return lib.startTestServer().then(server => {
					testServer = server;
					return args;
				});
			})
			.then(function () {
				return lib.listTestTables(args).then(function (res) {
					SUBJECT = SUBJECT.set('initialTableList', Immutable.fromJS(res));
					return null;
				});
			})

			// Run the first migrateUp() without indexes.
			.then(function () {
				return lib.migrateUp(args);
			})
			.then(function () {
				return lib.describeTestTables(args).then(function (res) {
					SUBJECT = SUBJECT.set('tables', Immutable.fromJS(res));
					return null;
				});
			})

			// Run the second migrateUp() adding new indexes.
			//
			// Because of the way Dynalite works, we need to close the test server,
			// and restart it for the next migration.
			.then(function () {
				return new Promise(function (resolve) {
					testServer.close(function () {
						resolve(null);
					});
				});
			})
			// Setup a new server instance.
			.then(() => {
				return lib.startTestServer().then(server => {
					testServer = server;
					return args;
				});
			})
			.then(function () {
				args.db = lib.createDbInstance(
					args,
					{
						Character: {
							indexes: {
								ByName: {
									keys: {
										hash: {name: 'type', type: 'String'},
										range: {name: 'name', type: 'String'}
									}
								}
							}
						},
						Series: {
							indexes: {
								ByTitle: {
									keys: {
										hash: {name: 'type', type: 'String'},
										range: {name: 'title', type: 'String'}
									}
								}
							}
						}
					}
				);
			})
			.then(function () {
				return lib.migrateUp(args);
			})
			.then(function () {
				return lib.describeTestTables(args).then(function (res) {
					SUBJECT = SUBJECT.set('withIndexes', Immutable.fromJS(res));
					return null;
				});
			})

			// Complete setup
			.then(done)
			.catch(done.fail);
	}, 500 * 1000); // Allow setup to run longer

	afterAll(function (done) {
		testServer.close(function () {
			done();
		});
	});

	it('initializes with no tables', function () {
		var list = SUBJECT.get('initialTableList');
		expect(list.size).toBe(0);
	});

	it('creates expected number of new tables', function () {
		var tables = SUBJECT.get('tables');
		expect(tables.size).toBe(3);
	});

	it('waits for new tables to become active', function () {
		var tables = SUBJECT.get('tables').toJS();
		tables.forEach(function (desc) {
			expect(desc.Table.TableStatus).toBe('ACTIVE');
		});
	});

	it('names tables correctly', function () {
		var tables = SUBJECT.get('tables')
			.toJS()
			.map(function (desc) {
				return desc.Table.TableName;
			})
			.sort();

		expect(tables).toEqual([
			'ddb_engine_tests_character_entities',
			'ddb_engine_tests_relations',
			'ddb_engine_tests_series_entities'
		]);
	});

	it('sets the entity table key schemas correctly', function () {
		var tables = SUBJECT.get('tables')
			.toJS()
			.filter(function (desc) {
				return /entities$/.test(desc.Table.TableName);
			});

		expect(tables.length).toBe(2);

		tables.forEach(function (desc) {
			var KeySchema = desc.Table.KeySchema;
			var AttributeDefinitions = desc.Table.AttributeDefinitions;

			expect(KeySchema[0])
				.toEqual({AttributeName: 'id', KeyType: 'HASH'});

			expect(AttributeDefinitions[0])
				.toEqual({AttributeName: 'id', AttributeType: 'S'});
		});
	});

	it('sets the relationship table key schemas correctly', function () {
		var tables = SUBJECT.get('tables')
			.toJS()
			.filter(function (desc) {
				return /relations$/.test(desc.Table.TableName);
			});

		expect(tables.length).toBe(1);

		var desc = tables[0];
		var KeySchema = desc.Table.KeySchema.sort(lib.sortByAttributeName);
		var AttributeDefinitions = desc.Table.AttributeDefinitions
			.sort(lib.sortByAttributeName);

		expect(KeySchema[0])
			.toEqual({AttributeName: 'objectId', KeyType: 'RANGE'});
		expect(KeySchema[1])
			.toEqual({AttributeName: 'subjectId', KeyType: 'HASH'});

		expect(AttributeDefinitions[0])
			.toEqual({AttributeName: 'objectId', AttributeType: 'S'});
		expect(AttributeDefinitions[1])
			.toEqual({AttributeName: 'objectType', AttributeType: 'S'});
		expect(AttributeDefinitions[2])
			.toEqual({AttributeName: 'subjectId', AttributeType: 'S'});
		expect(AttributeDefinitions[3])
			.toEqual({AttributeName: 'subjectType', AttributeType: 'S'});
	});

	it('creates indexes for relations', function () {
		var tables = SUBJECT.get('tables')
			.toJS()
			.filter(function (desc) {
				return /relations$/.test(desc.Table.TableName);
			});

		expect(tables.length).toBe(1);

		var desc = tables[0];

		var indexNames = desc.Table.GlobalSecondaryIndexes
			.map(function (desc) {
				return desc.IndexName;
			})
			.sort();

		expect(indexNames).toEqual([
			'ddb_engine_tests_belongs_to',
			'ddb_engine_tests_has_many'
		]);

		var indexStatus = desc.Table.GlobalSecondaryIndexes
			.map(function (desc) {
				return desc.IndexStatus;
			});

		expect(indexStatus).toEqual(['ACTIVE', 'ACTIVE']);
	});

	describe('after indexes added', function () {
		function mapGsi(gsiDesc) {
			var KeySchema = gsiDesc.KeySchema.sort(lib.sortByAttributeName);
			return {
				IndexName: gsiDesc.IndexName,
				IndexStatus: gsiDesc.IndexStatus,
				KeySchema: KeySchema
			};
		}

		beforeAll(function () {
			// Map the AWS responses into something we can more easily test.
			this.tables = SUBJECT.get('withIndexes')
				.toJS()
				.map(function (desc) {
					var AttributeDefinitions = desc.Table.AttributeDefinitions
						.sort(lib.sortByAttributeName);

					var GSI = (desc.Table.GlobalSecondaryIndexes || []).map(mapGsi);

					return {
						TableName: desc.Table.TableName,
						TableStatus: desc.Table.TableStatus,
						AttributeDefinitions: AttributeDefinitions,
						GlobalSecondaryIndexes: GSI
					};
				})
				.sort(function (a, b) {
					return a.TableName > b.TableName ? 1 : -1;
				});
		});

		it('returns the correct tables', function () {
			var tableNames = this.tables
				.filter(function (desc) {
					return /entities$/.test(desc.TableName);
				})
				.map(function (desc) {
					return desc.TableName;
				});

			expect(tableNames).toEqual([
				'ddb_engine_tests_character_entities',
				'ddb_engine_tests_series_entities'
			]);
		});

		it('correctly names the indexes', function () {
			var indexNames = this.tables.reduce(function (indexNames, desc) {
				return indexNames.concat(
					desc.GlobalSecondaryIndexes.map(pluckIndexName)
				);
			}, []);

			function pluckIndexName(gsi) {
				return gsi.IndexName;
			}

			expect(indexNames).toEqual([
				'ddb_engine_tests_character_by_name',
				'ddb_engine_tests_has_many',
				'ddb_engine_tests_belongs_to',
				'ddb_engine_tests_series_by_title'
			]);
		});

		it('has the required AttributeDefinitions', function () {
			var defs = this.tables
				.filter(function (desc) {
					return /entities$/.test(desc.TableName);
				})
				.reduce(function (defs, desc) {
					return defs.concat(desc.AttributeDefinitions);
				}, []);

			expect(defs[0]).toEqual({AttributeName: 'id', AttributeType: 'S'});
			expect(defs[1]).toEqual({AttributeName: 'name', AttributeType: 'S'});
			expect(defs[2]).toEqual({AttributeName: 'type', AttributeType: 'S'});
			expect(defs[3]).toEqual({AttributeName: 'id', AttributeType: 'S'});
			expect(defs[4]).toEqual({AttributeName: 'title', AttributeType: 'S'});
			expect(defs[5]).toEqual({AttributeName: 'type', AttributeType: 'S'});
		});

		it('has the correct KeySchema', function () {
			var keys = this.tables
				.filter(function (desc) {
					return /entities$/.test(desc.TableName);
				})
				.reduce(function (keys, desc) {
					return keys.concat(
						U.flatten(desc.GlobalSecondaryIndexes.map(pluckKeySchema))
					);
				}, []);

			function pluckKeySchema(gsi) {
				return gsi.KeySchema;
			}

			expect(keys[0]).toEqual({AttributeName: 'name', KeyType: 'RANGE'});
			expect(keys[1]).toEqual({AttributeName: 'type', KeyType: 'HASH'});
			expect(keys[2]).toEqual({AttributeName: 'title', KeyType: 'RANGE'});
			expect(keys[3]).toEqual({AttributeName: 'type', KeyType: 'HASH'});
		});

		it('waits for tables and indexes to become active', function () {
			var status = this.tables
				.filter(function (desc) {
					return /entities$/.test(desc.TableName);
				})
				.reduce(function (keys, desc) {
					keys.push(desc.TableStatus);
					return keys.concat(
						U.flatten(desc.GlobalSecondaryIndexes.map(pluckIndexStatus))
					);
				}, []);

			function pluckIndexStatus(gsi) {
				return gsi.IndexStatus;
			}

			expect(status.length).toBe(4);
			expect(status).toEqual(['ACTIVE', 'ACTIVE', 'ACTIVE', 'ACTIVE']);
		});
	});
});
