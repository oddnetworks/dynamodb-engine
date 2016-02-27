/* global beforeAll */
'use strict';
var U = require('../../lib/utils');

var envDefaults = {
	AWS_ACCESS_KEY_ID: 'default AWS_ACCESS_KEY_ID',
	AWS_SECRET_ACCESS_KEY: 'default AWS_SECRET_ACCESS_KEY',
	AWS_REGION: 'default AWS_REGION',
	DYNAMODB_ENDPOINT: 'http://localhost:8000'
};

var env = Object.keys(envDefaults).reduce(function (env, key) {
	env[key] = process.env[key] || envDefaults[key];
	return env;
}, Object.create(null));

var constants = U.deepFreeze({
	TABLE_PREFIX: 'ddb_engine_tests'
});

beforeAll(function () {
	this.constants = U.deepFreeze(U.extend(
		Object.create(null),
		env,
		constants
	));
});
