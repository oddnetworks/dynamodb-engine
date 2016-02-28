'use strict';

var U = require('../../lib/utils');
var DynamoDBEngine = require('../../lib/dynamodb-engine');
var debug = require('debug')('dynamodb-engine:test');

var lib = exports;

lib.initializeDb = function (args) {
	args = args.set('db', lib.createDbInstance(args));

	function returnArgs() {
		return args;
	}

	return Promise.resolve(args)
		.then(lib.removeTestTables)
		.then(returnArgs)
		.then(lib.migrateUp);
};

lib.createDbInstance = function (args, schema) {
	schema = schema || args.get('SCHEMA');

	return DynamoDBEngine.create({
		accessKeyId: args.get('AWS_ACCESS_KEY_ID'),
		secretAccessKey: args.get('AWS_SECRET_ACCESS_KEY'),
		region: args.get('AWS_REGION'),
		endpoint: args.get('DYNAMODB_ENDPOINT'),
		tablePrefix: args.get('TABLE_PREFIX')
	}, schema);
};

lib.removeTestTables = function (args) {
	debug('removeTestTables');
	return lib.listTestTables(args).then(function (tableNames) {
		if (tableNames.length) {
			return Promise.all(tableNames.map(U.partial(lib.deleteTable, args)))
				.then(U.constant(tableNames));
		}
		return tableNames;
	});
};

lib.deleteTable = function (args, tableName) {
	debug('deleteTable %s', tableName);
	var dynamodb = args.get('db').dynamodb;
	return dynamodb.deleteTable({TableName: tableName});
};

lib.listTestTables = function (args) {
	var dynamodb = args.get('db').dynamodb;
	var tablePrefix = args.get('TABLE_PREFIX');

	return dynamodb.listTables().then(function (res) {
		return res.TableNames.filter(function (tableName) {
			return tableName.indexOf(tablePrefix) >= 0;
		});
	});
};

lib.migrateUp = function (args) {
	debug('migrateUp');
	var db = args.get('db');
	return db.migrateUp();
};

lib.describeTestTables = function describeTestTables(args) {
	var dynamodb = args.get('db').dynamodb;

	return lib.listTestTables(args).then(function (tableNames) {
		return Promise.all(tableNames.map(function (tableName) {
			return dynamodb.describeTable({TableName: tableName});
		}));
	});
};

lib.sortByAttributeName = function (a, b) {
	return a.AttributeName > b.AttributeName ? 1 : -1;
};
