'use strict';
const Promise = require('bluebird');
const test = require('tape');
const debug = require('debug')('integration');
const FilePath = require('filepath');
const U = require('../lib/utils');

const DynamoDBEngine = require('../lib/dynamodb-engine');

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

const SCHEMA = U.deepFreeze({
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
});

const DB = DynamoDBEngine.create({
	accessKeyId: ACCESS_KEY_ID,
	secretAccessKey: SECRET_ACCESS_KEY,
	region: REGION,
	endpoint: ENDPOINT,
	tablePrefix: 'integration_test'
}, SCHEMA);

debug('Using ENDPOINT %s', ENDPOINT);

function die(err) {
	console.log('Fatal error:');
	console.log(err.stack || err);
	process.exit(1);
}

function createSeries(data) {
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
}

function createCharacter(data) {
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
}

// test('manually cleanup from failed tests', function (t) {

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

test('remove leftover test tables if any', function (t) {
	var tableNames = Object.keys(SCHEMA).map(function (key) {
		return DB.dynamodb.table(key);
	});
	tableNames.push(DB.dynamodb.relationTable());

	var promises = tableNames.map(function (tableName) {
		return DB.dynamodb.deleteTable({TableName: tableName});
	});

	Promise.all(promises)
		.then(function () {})
		.catch(DB.NotFoundError, function () {})
		.catch(die)
		.then(t.end);
});

test('sanity check to make sure tables do not exist', function (t) {
	var tableNames = Object.keys(SCHEMA).map(function (key) {
		return DB.dynamodb.table(key);
	});
	tableNames.push(DB.dynamodb.relationTable());

	t.plan(3);

	DB.dynamodb.listTables()
		.then(function (res) {
			tableNames.forEach(function (name) {
				t.equal(res.TableNames.indexOf(name), -1, `check table ${name}`);
			});
		})
		.catch(die)
		.then(t.end);
});

test('createRecord on table that does not exist', function (t) {
	t.plan(2);

	DB.createRecord({id: 'foo', type: 'Character'})
		.then(function () {
			t.fail('then() callback should not be called');
			t.end();
		})
		.catch(DB.OperationalError, function (err) {
			t.ok(/table for Character does not exist/.test(err.message));
			t.ok(/migration([\s\w]+)required/.test(err.message));
		})
		.catch(die)
		.then(t.end);
});

test('updateRecord on table that does not exist', function (t) {
	t.plan(2);

	DB.updateRecord({id: 'foo', type: 'Character'})
		.then(function () {
			t.fail('then() callback should not be called');
			t.end();
		})
		.catch(DB.OperationalError, function (err) {
			t.ok(/table for Character does not exist/.test(err.message));
			t.ok(/migration([\s\w]+)required/.test(err.message));
		})
		.catch(die)
		.then(t.end);
});

test('createRelation on table that does not exist', function (t) {
	t.plan(2);

	DB.createRelation('bar', {id: 'foo', type: 'Character'})
		.then(function () {
			t.fail('then() callback should not be called');
			t.end();
		})
		.catch(DB.OperationalError, function (err) {
			t.ok(/table for relations does not exist/.test(err.message));
			t.ok(/migration([\s\w]+)required/.test(err.message));
		})
		.catch(die)
		.then(t.end);
});

test('getRecord on table that does not exist', function (t) {
	t.plan(2);

	DB.getRecord('Character', 'foo')
		.then(function () {
			t.fail('then() callback should not be called');
			t.end();
		})
		.catch(DB.OperationalError, function (err) {
			t.ok(/table for Character does not exist/.test(err.message));
			t.ok(/migration([\s\w]+)required/.test(err.message));
		})
		.catch(die)
		.then(t.end);
});

test('removeRecord on table that does not exist', function (t) {
	t.plan(2);

	DB.removeRecord('Character', 'foo')
		.then(function () {
			t.fail('then() callback should not be called');
			t.end();
		})
		.catch(DB.OperationalError, function (err) {
			t.ok(/table for Character does not exist/.test(err.message));
			t.ok(/migration([\s\w]+)required/.test(err.message));
		})
		.catch(die)
		.then(t.end);
});

test('query invalid table', function (t) {
	var query = DB.query({
		type: 'Character',
		index: 'ByName'
	});

	var tableName = DB.dynamodb.table('Character');
	t.plan(2);

	query
		.hashEqual(47)
		.rangeEqual('baz')
		.fetchAll()
		.catch(DB.OperationalError, function (err) {
			t.ok(tableName.length > 0, 'tableName exists');
			t.notEqual(err.message.indexOf(tableName), -1, 'err message');
		})
		.catch(die)
		.then(t.end);
});

test('migrateUp', function (t) {
	t.plan(4);

	DB.migrateUp()
		.then(function (res) {
			t.equal(res.length, 3);
			res.forEach(function (table) {
				t.equal(typeof table.TableArn, 'string');
			});
		})
		.catch(die);
});

test('query invalid index', function (t) {
	t.plan(1);

	t.throws(function () {
		DB.query({
			type: 'Character',
			index: 'ByFoobar'
		});
	}, DB.OperationalError);
});

test('populate series data', function (t) {
	const dir = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel', 'series');

	function processDocument(file) {
		return file.read()
			.then(JSON.parse)
			.then(createSeries)
			.then(DB.createRecord.bind(DB));
	}

	dir
		.list()
		.reduce(function (promise, file) {
			return promise.then(function () {
				return processDocument(file);
			});
		}, Promise.resolve(null))
		.catch(die)
		.then(function () {
			t.end();
		});
});

test('populate character data', function (t) {
	const dir = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel', 'characters');

	function createRelationships(character) {
		var promises = character.series.map(function (seriesId) {
			return DB.getRecord('Series', seriesId)
				.then(function createRelationship(series) {
					return DB.createRelation(series.id, character);
				})
				.catch(DB.NotFoundError, function () {});
		});

		return Promise.all(promises).then(U.constant(character));
	}

	function processDocument(file) {
		return file.read()
			.then(JSON.parse)
			.then(createCharacter)
			.then(DB.updateRecord.bind(DB))
			.then(createRelationships);
	}

	dir
		.list()
		.reduce(function (promise, file) {
			return promise.then(function () {
				return processDocument(file);
			});
		}, Promise.resolve(null))
		.catch(die)
		.then(function () {
			t.end();
		});
});

test('get and delete related items', function (t) {
	t.plan(5);

	function getInitialRelations() {
		return DB.getRelations('10051', 'Character')
			.then(function (res) {
				t.notEqual(res.indexOf('1011360'), -1);
				t.notEqual(res.indexOf('1009608'), -1);
			});
	}

	function removeOneRelation() {
		return DB.removeRelation('10051', '1011360').then(function (res) {
			t.equal(res, true, 'removeRelation response is true');
		});
	}

	function getFinalRelations() {
		return DB.getRelations('10051', 'Character')
			.then(function (res) {
				t.equal(res.indexOf('1011360'), -1);
				t.notEqual(res.indexOf('1009608'), -1);
			});
	}

	getInitialRelations()
		.then(removeOneRelation)
		.then(getFinalRelations)
		.catch(die)
		.then(t.end);
});

test('batch get items', function (t) {
	const files = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel', 'characters')
		.list();

	t.plan(3);

	const records = DB.batchGetRecords({
		// Add a record id 'foobar' that will not be found -- should be
		// silently ignored.
		Character: ['foobar'].concat(files.map(function (file) {
			return file.basename('.json');
		}))
	});

	records
		.then(function (res) {
			// The not found item 'foobar' is not included in the results.
			t.equal(res.Character.length, files.length);
			t.equal(res.Character[0].id, '1009148');
			t.equal(res.Character[0].type, 'Character');
		})
		.catch(die)
		.then(t.end);
});

test('query an index', function (t) {
	var query = DB.query({
		type: 'Character',
		index: 'ByName'
	});

	t.plan(2);

	query
		.hashEqual('Character')
		.rangeBeginsWith('A')
		.fetchAll()
		.then(function (results) {
			t.equal(results.length, 77, 'results length');
			t.equal(results[0].name, 'A-Bomb (HAS)');
		})
		.catch(die)
		.then(t.end);
});

test('add a new table', function (t) {
	const thisSchema = U.extend(Object.create(null), {
		Widget: {}
	}, SCHEMA);

	const thisDB = DynamoDBEngine.create({
		accessKeyId: ACCESS_KEY_ID,
		secretAccessKey: SECRET_ACCESS_KEY,
		region: REGION,
		endpoint: ENDPOINT,
		tablePrefix: 'integration_test'
	}, thisSchema);

	var tableName = DB.dynamodb.table('Widget');

	t.plan(6);

	function sanityCheck() {
		// Make sure the table does not already exist.
		return thisDB.dynamodb.listTables()
			.then(function (res) {
				t.equal(res.TableNames.indexOf(tableName), -1, 'check for Widget table');
			});
	}

	function migrateUp() {
		return thisDB.migrateUp()
			.then(function (res) {
				t.equal(res.length, 4);
				res.forEach(function (table) {
					t.equal(typeof table.TableArn, 'string');
				});
			});
	}

	function removeTable() {
		return thisDB.dynamodb.deleteTable({TableName: tableName});
	}

	sanityCheck()
		.then(migrateUp)
		.then(removeTable)
		.catch(die);
});
