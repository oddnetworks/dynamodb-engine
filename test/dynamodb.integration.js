'use strict';
const Promise = require('bluebird');
const test = require('tape');
const debug = require('debug')('integration');
const FilePath = require('filepath');

const Database = require('../lib/database');
const Table = require('../lib/table');
const errors = require('../lib/errors');
const constants = require('../lib/constants');

const ACCESS_KEY_ID = process.env.ACCESS_KEY_ID;
const SECRET_ACCESS_KEY = process.env.SECRET_ACCESS_KEY;
const REGION = process.env.REGION;

// The localhost ENDPOINT should reference DynamoDB Local.
// For more info see here:
//   http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
const ENDPOINT = process.env.ENDPOINT || 'http://localhost:8000';

if (!ACCESS_KEY_ID) {
	console.error('ACCESS_KEY_ID env variable is required.');
	process.exit(1);
}
if (!SECRET_ACCESS_KEY) {
	console.error('SECRET_ACCESS_KEY env variable is required.');
	process.exit(1);
}
if (!REGION) {
	console.error('REGION env variable is required.');
	process.exit(1);
}

const DYNAMODB_OPTIONS = Object.create(null);
DYNAMODB_OPTIONS.accessKeyId = ACCESS_KEY_ID;
DYNAMODB_OPTIONS.secretAccessKey = SECRET_ACCESS_KEY;
DYNAMODB_OPTIONS.region = REGION;
DYNAMODB_OPTIONS.endpoint = ENDPOINT;
Object.freeze(DYNAMODB_OPTIONS);

const ENTITIES_TABLE = 'test_entities_test';
const RELATIONSHIPS_TABLE = 'test_relationships_test';

debug('Using ENDPOINT %s', ENDPOINT);
debug('Using ENTITIES_TABLE %s', ENTITIES_TABLE);
debug('Using RELATIONSHIPS_TABLE %s', RELATIONSHIPS_TABLE);

function die(err) {
	console.log('Fatal error:');
	console.log(err);
	process.exit(1);
}

// test('cleanup from failed tests', function (t) {
// 	var db = Database.create(DYNAMODB_OPTIONS);

// 	function onsuccess(res) {
// 		console.log('SUCCESS:');
// 		console.log(res);
// 	}

// 	function onerror(err) {
// 		console.log('ERROR:');
// 		console.log(err);
// 	}

// 	debugger;
// });

test.skip('sanity check: test table does not exist', function (t) {
	Database.create(DYNAMODB_OPTIONS)
		.listTables()
		.then(function (res) {
			t.equal(res.indexOf(ENTITIES_TABLE), -1);
			t.equal(res.indexOf(RELATIONSHIPS_TABLE), -1);

			if (res.indexOf(ENTITIES_TABLE) !== -1) {
				console.error('%s table already exists.', ENTITIES_TABLE);
				console.error('You\'ll need to delete it using the AWS console or the REPL');
				process.exit(1);
			}
			if (res.indexOf(RELATIONSHIPS_TABLE) !== -1) {
				console.error('%s table already exists.', RELATIONSHIPS_TABLE);
				console.error('You\'ll need to delete it using the AWS console or the REPL');
				process.exit(1);
			}
		})
		.catch(die)
		.then(t.end);
});

test.skip('get item from table which does not exist', function (t) {
	Database.create(DYNAMODB_OPTIONS).useTable(ENTITIES_TABLE)
		.get({id: 'foo'})
		.catch(errors.NonExistentTableError, function (err) {
			t.equal(err.code, constants.get('NONEXISTENT_TABLE_ERROR'));
		})
		.catch(die)
		.then(t.end);
});

test.skip('create entities table with 1 global index', function (t) {
	Database.create(DYNAMODB_OPTIONS)
		.createTable({
			tableName: ENTITIES_TABLE,
			attributes: {
				id: 'String',
				type: 'String',
				modified: 'String'
			},
			keys: {
				hash: 'id',
				range: 'modified'
			},
			globalIndexes: [
				{
					indexName: ENTITIES_TABLE + '_type',
					keys: {
						hash: 'type',
						range: 'modified'
					}
				}
			]
		})
		.then(function (res) {
			const gsi = res.GlobalSecondaryIndexes[0];

			t.equal(res.TableStatus, 'ACTIVE');
			t.equal(res.ItemCount, 0);
			t.equal(res.TableArn.split('/').pop(), ENTITIES_TABLE);

			t.equal(gsi.IndexStatus, 'ACTIVE');
			t.equal(gsi.ItemCount, 0);
			t.equal(gsi.IndexArn.split('/').pop(), ENTITIES_TABLE + '_type');
		})
		.catch(die)
		.then(t.end);
});

test.skip('create relationships table with 1 global index', function (t) {
	Database.create(DYNAMODB_OPTIONS)
		.createTable({
			tableName: RELATIONSHIPS_TABLE,
			attributes: {
				subject: 'String',
				relation: 'String',
				target: 'String'
			},
			keys: {
				hash: 'target',
				range: 'subject'
			},
			globalIndexes: [
				{
					indexName: RELATIONSHIPS_TABLE + '_relation',
					keys: {
						hash: 'subject',
						range: 'relation'
					}
				},
				{
					indexName: RELATIONSHIPS_TABLE + '_reverse_relation',
					keys: {
						hash: 'target',
						range: 'relation'
					}
				}
			]
		})
		.then(function (res) {
			const gsi = res.GlobalSecondaryIndexes;
			t.equal(gsi.length, 2);
		})
		.catch(die)
		.then(t.end);
});

test.skip('populate entities table', function (t) {
	const table = Database.create(DYNAMODB_OPTIONS).useTable(ENTITIES_TABLE);

	let characters = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel', 'characters')
		.list();

	let comics = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel', 'comics')
		.list();

	const files = characters.concat(comics);

	function postDocument(rec) {
		// Throttle requests on ThroughputExceededError
		var promise = table.put(rec, {
			condition: 'attribute_not_exists(id)'
		});

		return promise.catch(errors.ThroughputExceededError, function () {
			debug('ThroughputExceededError');
			return Promise.delay(2000).then(function () {
				return postDocument(rec);
			});
		});
	}

	function mapComic(doc) {
		return {
			id: doc.id.toString(),
			type: 'Comic',
			title: doc.title || 'EMPTY',
			description: doc.description || 'EMPTY',
			modified: doc.modified || 'EMPTY',
			issueNumber: parseInt(doc.issueNumber, 10) || 0,
			upc: doc.upc || 'EMPTY',
			pageCount: parseInt(doc.pageCount, 10) || 10
		};
	}

	function mapCharacter(doc) {
		return {
			id: doc.id.toString(),
			type: 'Character',
			name: doc.name || 'EMPTY',
			description: doc.description || 'EMPTY',
			modified: doc.modified || 'EMPTY',
			thumbnail: doc.thumbnail.path + '.' + doc.thumbnail.extension,
			comicsAvailable: doc.comics.items.length,
			comicsUri: doc.comics.collectionURI,
			comics: doc.comics.items.map(function (item) {
				var parts = item.resourceURI.split('/');
				var id = parts.pop();
				if (id) {
					return id;
				}
				return parts.pop();
			})
		};
	}

	function mapDocument(doc) {
		if (doc.hasOwnProperty('isbn') || doc.hasOwnProperty('upc')) {
			return mapComic(doc);
		}
		return mapCharacter(doc);
	}

	// Read, map, and post documents serially.
	files.reduce(function (promise, file) {
		return promise
			.then(function () {
				return file.read();
			})
			.then(function parseJSON(text) {
				return JSON.parse(text);
			})
			.then(mapDocument)
			.then(postDocument);
	}, Promise.resolve(null))
	.catch(function (err) {
		debug(err);
		t.error(err);
	})
	.then(function () {
		t.end();
	});
});

test('populate relationships table', function (t) {
	const database = Database.create(DYNAMODB_OPTIONS);
	const entitiesTable = Table.create({
		tableName: ENTITIES_TABLE,
		dynamodb: database.dynamodb,
		hashkey: 'type',
		rangekey: 'modified'
	});
	const query = entitiesTable.query({
		indexName: ENTITIES_TABLE + '_type'
	});

	query
		.hashEqual('Comic')
		.fetch()
		.then(function (res) {
			debugger;
		})
		.catch(function (err) {
			debugger;
		})
		.then(function () {
			t.end();
		});
});

test.skip('delete all tables', function (t) {
	Database.create(DYNAMODB_OPTIONS)
		.deleteTable({tableName: ENTITIES_TABLE})
		.then(function (res) {
			t.equal(res.TableName, ENTITIES_TABLE);
		})
		.catch(die)
		.then(t.end);
});
