'use strict';
const test = require('tape');
const debug = require('debug')('integration');
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
					hash: 'id',
					range: 'name'
				}
			}
		}
	},
	Comic: {
		indexes: {
			ByTitle: {
				keys: {
					hash: 'id',
					range: 'title'
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
});

debug('Using ENDPOINT %s', ENDPOINT);

function die(err) {
	console.log('Fatal error:');
	console.log(err);
	process.exit(1);
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

test.skip('sanity check to make sure tables do not exist', function (t) {
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

test.skip('createRecord on table that does not exist', function (t) {
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
