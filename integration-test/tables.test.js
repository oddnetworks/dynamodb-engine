'use strict';
const test = require('tape');
const DynamoDBEngine = require('../lib/dynamodb-engine');

const ACCESS_KEY_ID = process.env.ACCESS_KEY_ID;
const SECRET_ACCESS_KEY = process.env.SECRET_ACCESS_KEY;
const REGION = process.env.REGION;
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

test('list tables', function (t) {
	const DB = createDynamoDBEngine();

	DB.listTables()
		.then(function (res) {
			console.log('Table Listing:');
			console.log(res);
			t.end();
		})
		.catch(function (err) {
			console.error('Error listing tables:');
			console.error(err.stack);
			process.exit(1);
		});
});

test('create a new table', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	DB.createTable()
	.then(function (res) {
		console.log('Table created:');
		console.log(res);
		t.end();
	})
	.catch(function (err) {
		console.error('Error creating the table:');
		console.error(err.stack);
		process.exit(1);
	});
});

test('list tables again', function (t) {
	const DB = createDynamoDBEngine();

	DB.listTables()
		.then(function (res) {
			console.log('Table Listing:');
			console.log(res);
			t.end();
		})
		.catch(function (err) {
			console.error('Error listing tables:');
			console.error(err.stack);
			process.exit(1);
		});
});

test('delete the new table', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	DB.deleteTable()
	.then(function (res) {
		console.log('Table deleted:');
		console.log(res);
		t.end();
	})
	.catch(function (err) {
		console.error('Error deleting the table:');
		console.error(err.stack);
		process.exit(1);
	});
});

test('delete the table again', function (t) {
	const DB = createDynamoDBEngine('table_test_table');

	DB.deleteTable()
	.then(function (res) {
		console.log('Table deleted:');
		console.log(res);
		t.end();
	})
	.catch(function (err) {
		console.error('Error deleting the table:');
		console.error(err.stack);
		process.exit(1);
	});
});

test('list tables last', function (t) {
	const DB = createDynamoDBEngine();

	DB.listTables()
		.then(function (res) {
			console.log('Table Listing:');
			console.log(res);
			t.end();
		})
		.catch(function (err) {
			console.error('Error listing tables:');
			console.error(err.stack);
			process.exit(1);
		});
});

