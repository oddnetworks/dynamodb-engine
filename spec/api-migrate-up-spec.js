/* global describe, beforeAll, it, expect */
'use strict';
var Immutable = require('immutable');
var Map = Immutable.Map;

var DynamoDBEngine = require('../lib/dynamodb-engine');

describe('API migrateUp()', function () {
	var subject = Object.create(null);
	var constants;

	beforeAll(function (done) {
		constants = this.constants;

		subject.db = DynamoDBEngine.create({
			accessKeyId: constants.AWS_ACCESS_KEY_ID,
			secretAccessKey: constants.AWS_SECRET_ACCESS_KEY,
			region: constants.AWS_REGION,
			endpoint: constants.DYNAMODB_ENDPOINT,
			tablePrefix: constants.TABLE_PREFIX
		}, {
			Character: {},
			Series: {}
		});

		var args = new Map({
			db: subject.db
		});

		Promise.resolve(args)
			.then(listDatabases)
			.then(function (res) {
				subject.initialTableList = res;
				return args;
			})
			.then(function () {
				Object.freeze(subject);
			})
			.then(done, done.fail);
	});

	it('initializes with no tables', function () {
		var list = subject.initialTableList.TableNames;
		expect(list.length).toBe(0);
	});
});

function listDatabases(args) {
	var dynamodb = args.get('db').dynamodb;
	return dynamodb.listTables();
}
