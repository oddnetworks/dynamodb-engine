/* global describe, beforeAll, it, expect */
'use strict';
var Immutable = require('immutable');
var Map = Immutable.Map;
var List = Immutable.List;

var U = require('../lib/utils');
var DynamoDBEngine = require('../lib/dynamodb-engine');

describe('API migrateUp()', function () {
	var subject = new Map();
	var constants;

	beforeAll(function (done) {
		constants = this.constants;

		var db = DynamoDBEngine.create({
			accessKeyId: constants.AWS_ACCESS_KEY_ID,
			secretAccessKey: constants.AWS_SECRET_ACCESS_KEY,
			region: constants.AWS_REGION,
			endpoint: constants.DYNAMODB_ENDPOINT,
			tablePrefix: constants.TABLE_PREFIX
		}, {
			Character: {},
			Series: {}
		});

		var args = U.constant(new Map(U.extend(
			Object.create(null),
			{db: db},
			constants
		)));

		Promise.resolve(args())
			.then(removeTestTables)
			.then(args)
			.then(listTestTables)
			.then(function (res) {
				subject = subject.set('initialTableList', new List(res));
			})
			.then(args)
			.then(migrateUp)
			.then(args)
			.then(describeTestTables)
			.then(function (res) {
				subject = subject.set('tables', Immutable.fromJS(res));
			})
			.then(function () {
				Object.freeze(subject);
			})
			.then(U.returnUndefined)
			.then(done, done.fail);
	}, 100 * 1000); // Allow setup to run longer

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
		var KeySchema = desc.Table.KeySchema.sort(sort);
		var AttributeDefinitions = desc.Table.AttributeDefinitions.sort(sort);

		function sort(a, b) {
			return a.AttributeName > b.AttributeName ? 1 : -1;
		}

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
});

function removeTestTables(args) {
	return listTestTables(args).then(function (tableNames) {
		if (tableNames.length) {
			return Promise.all(tableNames.map(U.partial(deleteTable, args)))
				.then(U.constant(tableNames));
		}
		return tableNames;
	});
}

function deleteTable(args, tableName) {
	var dynamodb = args.get('db').dynamodb;
	return dynamodb.deleteTable({TableName: tableName});
}

function listTestTables(args) {
	var dynamodb = args.get('db').dynamodb;
	var tablePrefix = args.get('TABLE_PREFIX');

	return dynamodb.listTables().then(function (res) {
		return res.TableNames.filter(function (tableName) {
			return tableName.indexOf(tablePrefix) >= 0;
		});
	});
}

function migrateUp(args) {
	var db = args.get('db');
	return db.migrateUp();
}

function describeTestTables(args) {
	var dynamodb = args.get('db').dynamodb;

	return listTestTables(args).then(function (tableNames) {
		return Promise.all(tableNames.map(function (tableName) {
			return dynamodb.describeTable({TableName: tableName});
		}));
	});
}
