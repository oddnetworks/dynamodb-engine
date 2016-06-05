'use strict';

var Promise = require('bluebird');
var filepath = require('filepath');
var debug = require('debug')('dynamodb-engine:test');

var U = require('../../lib/utils');
var DynamoDBEngine = require('../../lib/dynamodb-engine');

var lib = exports;

lib.initializeDb = function (args) {
	args = args.set('db', lib.createDbInstance(args));

	function returnArgs() {
		return args;
	}

	return Promise.resolve(args)
		.then(lib.removeTestTables)
		.then(returnArgs)
		.then(lib.migrateUp)
		.then(returnArgs);
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

lib.readJsonFile = function (path) {
	return path.read().then(function (text) {
		return JSON.parse(text);
	});
};

lib.mapFixtureToRecord = function (data) {
	return data.title ? lib.createSeries(data) : lib.createCharacter(data);
};

lib.createSeries = function (data) {
	return {
		id: data.id.toString(),
		type: 'Series',
		title: data.title || 'EMPTY',
		description: data.description || 'EMPTY',
		resourceURI: data.resourceURI || 'EMPTY',
		startYear: data.startYear || 0,
		endYear: data.endYear || 0,
		urls: data.urls || [],
		creators: data.creators || {}
	};
};

lib.createCharacter = function (data) {
	var thumb = (data.thumbnail).path ? data.thumbnail : null;
	return {
		id: data.id.toString(),
		type: 'Character',
		name: data.name || 'EMPTY',
		description: data.description || 'EMPTY',
		resourceURI: data.resourceURI || 'EMPTY',
		emptyArray: [],
		emptyObject: {},
		thumbnail: {
			url: thumb ? (thumb.path + '.' + thumb.extension) : 'EMPTY',
			def: data.thumbnail
		},
		series: (data.series.items || []).map(function (item) {
			return item.resourceURI ? item.resourceURI.split('/').pop() : 'EMPTY';
		})
	};
};

lib.putItem = function (args, record) {
	return args.get('db').updateRecord(record);
};

lib.listFixturePaths = function (filter) {
	var fixturePath = filepath.create().append('fixtures');

	function filterByJson(path) {
		return /\.json$/.test(path.toString());
	}

	function reducePaths(filepaths, path) {
		if (path.isFile() && filterByJson(path)) {
			if (filter) {
				var pathPart = path.toString().slice(fixturePath.toString().length);
				if (filter(pathPart)) {
					filepaths.push(path);
				}
			} else {
				filepaths.push(path);
			}
		}
		if (path.isDirectory()) {
			filepaths = path.list().reduce(reducePaths, filepaths);
		}
		return filepaths;
	}

	return fixturePath.list().reduce(reducePaths, []);
};

lib.sortByAttributeName = function (a, b) {
	return a.AttributeName > b.AttributeName ? 1 : -1;
};
