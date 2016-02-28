/* global describe, beforeAll, it, expect */
'use strict';
var Promise = require('bluebird');
var Immutable = require('immutable');
var Map = Immutable.Map;
var List = Immutable.List;

describe('API migrateUp()', function () {
	var subject = new Map();
	var constants;
	var lib;
	var U;

	beforeAll(function (done) {
		constants = this.constants;
		lib = this.lib;
		U = this.U;

		var args = new Map(constants);

		// Initial DB schema without indexes.
		args = args.set('db', lib.createDbInstance(
			args,
			{Character: {}, Series: {}}
		));

		function returnArgs() {
			return args;
		}

		Promise.resolve(args)

			// Clean up from previous tests.
			.then(lib.removeTestTables)
			.then(returnArgs)
			.then(lib.listTestTables)
			.then(function (res) {
				subject = subject.set('initialTableList', new List(res));
			})

			// Run the first migrateUp() without indexes.
			.then(returnArgs)
			.then(lib.migrateUp)
			.then(returnArgs)
			.then(lib.describeTestTables)
			.then(function (res) {
				subject = subject.set('tables', Immutable.fromJS(res));
			})

			// Run the second migrateUp() adding new indexes.
			.then(function () {
				args = args.set('db', lib.createDbInstance(
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
				));
			})
			.then(returnArgs)
			.then(lib.migrateUp)
			.then(returnArgs)
			.then(lib.describeTestTables)
			.then(function (res) {
				subject = subject.set('withIndexes', Immutable.fromJS(res));
			})

			// Complete setup
			.then(U.returnUndefined)
			.then(done, done.fail);
	}, 240 * 1000); // Allow setup to run longer

	it('initializes with no tables', function () {
		var list = subject.get('initialTableList');
		expect(list.size).toBe(0);
	});

	it('creates expected number of new tables', function () {
		var tables = subject.get('tables');
		expect(tables.size).toBe(3);
	});

	it('waits for new tables to become active', function () {
		var tables = subject.get('tables').toJS();
		tables.forEach(function (desc) {
			expect(desc.Table.TableStatus).toBe('ACTIVE');
		});
	});

	it('names tables correctly', function () {
		var tables = subject.get('tables')
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
		var tables = subject.get('tables')
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
		var tables = subject.get('tables')
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
			.toEqual({AttributeName: 'object', KeyType: 'RANGE'});
		expect(KeySchema[1])
			.toEqual({AttributeName: 'subject', KeyType: 'HASH'});

		expect(AttributeDefinitions[0])
			.toEqual({AttributeName: 'object', AttributeType: 'S'});
		expect(AttributeDefinitions[1])
			.toEqual({AttributeName: 'predicate', AttributeType: 'S'});
		expect(AttributeDefinitions[2])
			.toEqual({AttributeName: 'subject', AttributeType: 'S'});
	});

	it('creates indexes for relations', function () {
		var tables = subject.get('tables')
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
			this.tables = subject.get('withIndexes')
				.toJS()
				.map(function (desc) {
					var AttributeDefinitions = desc.Table.AttributeDefinitions
						.sort(lib.sortByAttributeName);

					var GSI = desc.Table.GlobalSecondaryIndexes.map(mapGsi);

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
