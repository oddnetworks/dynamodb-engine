'use strict';

var Promise = require('bluebird');
var filepath = require('filepath');

var U = require('../../lib/utils');
var DynamoDBEngine = require('../../lib/dynamodb-engine');

var lib = exports;

// args.AWS_ACCESS_KEY_ID
// args.AWS_SECRET_ACCESS_KEY
// args.AWS_REGION
// args.DYNAMODB_ENDPOINT
// args.TABLE_PREFIX
// args.SCHEMA
lib.initializeDb = function (args) {
	args = U.cloneDeep(args);
	args.db = lib.createDbInstance(args);

	return Promise.resolve(args)
		.then(lib.removeTestTables)
		.then(U.constant(args))
		.then(lib.migrateUp)
		.then(U.constant(args));
};

// args.AWS_ACCESS_KEY_ID
// args.AWS_SECRET_ACCESS_KEY
// args.AWS_REGION
// args.DYNAMODB_ENDPOINT
// args.TABLE_PREFIX
// args.SCHEMA
lib.createDbInstance = function (args, schema) {
	schema = schema || args.SCHEMA;

	return DynamoDBEngine.create({
		accessKeyId: args.AWS_ACCESS_KEY_ID,
		secretAccessKey: args.AWS_SECRET_ACCESS_KEY,
		region: args.AWS_REGION,
		endpoint: args.DYNAMODB_ENDPOINT,
		tablePrefix: args.TABLE_PREFIX
	}, schema);
};

// args.db.dynamodb
// args.TABLE_PREFIX
lib.removeTestTables = function (args) {
	return lib.listTestTables(args).then(function (tableNames) {
		if (tableNames.length) {
			return Promise.all(tableNames.map(U.partial(lib.deleteTable, args)))
				.then(U.constant(tableNames));
		}
		return tableNames;
	});
};

// args.db.dynamodb
lib.deleteTable = function (args, tableName) {
	return args.db.dynamodb.deleteTable({TableName: tableName});
};

// args.db.dynamodb
// args.TABLE_PREFIX
lib.listTestTables = function (args) {
	var tablePrefix = args.TABLE_PREFIX;

	return args.db.dynamodb.listTables().then(function (res) {
		return res.TableNames.filter(function (tableName) {
			return tableName.indexOf(tablePrefix) >= 0;
		});
	});
};

// args.db
lib.migrateUp = function (args) {
	return args.db.migrateUp();
};

// args.db.dynamodb
// args.TABLE_PREFIX
lib.describeTestTables = function describeTestTables(args) {
	var dynamodb = args.db.dynamodb;

	return lib.listTestTables(args).then(function (tableNames) {
		return Promise.all(tableNames.map(function (tableName) {
			return dynamodb.describeTable({TableName: tableName});
		}));
	});
};

// path - A Filepath instance
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

// args.db
lib.putItem = function (args, record) {
	return args.db.updateRecord(record);
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
