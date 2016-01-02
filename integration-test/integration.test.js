'use strict';
const Promise = require('bluebird');
const test = require('tape');
const FilePath = require('filepath');
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

console.log('Using ENDPOINT %s', ENDPOINT);

function createDynamoDBEngine(tableName) {
	const DB = DynamoDBEngine.create({
		accessKeyId: ACCESS_KEY_ID,
		secretAccessKey: SECRET_ACCESS_KEY,
		region: REGION,
		endpoint: ENDPOINT,
		tableName: tableName
	});

	DB.on('log', function (log) {
		console.log('LOG - %s - %s', log.level, log.message);
	});

	return DB;
}

function die(err) {
	console.log('Fatal error:');
	console.log(err);
	process.exit(1);
}

test('list tables', function (t) {
	const DB = createDynamoDBEngine();

	DB.listTables()
		.then(function (res) {
			console.log('Table Listing:');
			console.log(res);
			t.equal(res.indexOf('table_test_table'), -1);
		})
		.catch(die)
		.then(t.end);
});

test('create a new table', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	DB.createTable()
	.then(function (res) {
		t.equal(res.TableName, 'table_test_table');
		t.equal(res.TableStatus, 'ACTIVE');
		console.log('Table created: %s', res.TableName);
	})
	.catch(function (err) {
		t.fail(err.message);
	})
	.then(t.end);
});

test('create the table again', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	const promise = new Promise(function (resolve) {
		DB.on('log', function (log) {
			if (log.level === 'WARN') {
				t.equal(log.message, 'attempting to create table; ' +
					'"table_test_table" does not exist');
				resolve();
			}
		});

		DB.createTable()
		.then(function (res) {
			t.equal(res.TableName, 'table_test_table');
			console.log('Table created: %s', res.TableName);
		})
		.catch(function (err) {
			console.error('Expected error creating the table: %s', err.code);
		})
		.then(resolve);
	});

	promise.then(t.end).catch(die);
});

test('populate records', function (t) {
	const DB = createDynamoDBEngine('table_test_table');
	const files = FilePath
		.create(__dirname)
		.append('fixtures', 'Marvel-characters')
		.list();

	Promise.all(files.map(function (file) {
		return file.read()
			.then(function parseJSON(text) {
				return JSON.parse(text);
			})
			.then(function mapDocument(doc) {
				const record = {
					marvelId: doc.id.toString(),
					name: doc.name,
					thumbnail: doc.thumbnail.path + '.' + doc.thumbnail.extension,
					uri: doc.resourceURI,
					comicsAvailable: doc.comics.items.length,
					comicsUri: doc.comics.collectionURI,
					comics: doc.comics.items.map(function (item) {
						return item.resourceURI;
					})
				};
				if (doc.description) {
					record.description = doc.description;
				}
				return record;
			})
			.then(function postDocument(item) {
				return DB.post({
					data: item
				});
			});
	}))
	.then(function (items) {
		t.ok(items.length > 0, 'items.length');
		t.end();
	})
	.catch(die);
});

test('delete the new table', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	DB.deleteTable()
	.then(function () {
		console.log('Table deleted');
	})
	.catch(die)
	.then(t.end);
});

test('delete the table again', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	const promise = new Promise(function (resolve) {
		DB.on('log', function (log) {
			if (log.level === 'WARN') {
				t.equal(log.message, 'attempting to delete table; ' +
					'"table_test_table" does not exist');
				resolve();
			}
		});

		DB.deleteTable()
		.then(function () {
			console.log('Table deleted');
		})
		.catch(function (err) {
			console.error('Expected error deleting the table: %s', err.code);
		})
		.then(resolve);
	});

	promise.then(t.end).catch(die);
});
