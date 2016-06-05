/* global beforeAll */
'use strict';
var debug = require('debug')('dynamodb-engine:test-setup');

var U = require('../../lib/utils');

var envDefaults = {
	AWS_ACCESS_KEY_ID: 'default AWS_ACCESS_KEY_ID',
	AWS_SECRET_ACCESS_KEY: 'default AWS_SECRET_ACCESS_KEY',
	AWS_REGION: 'default AWS_REGION',
	DYNAMODB_ENDPOINT: 'http://localhost:8000',
	TABLE_PREFIX: 'ddb_engine_tests'
};

var env = Object.keys(envDefaults).reduce(function (env, key) {
	env[key] = process.env[key] || envDefaults[key];
	return env;
}, Object.create(null));

beforeAll(function () {
	debug('AWS_ACCESS_KEY_ID=%s', env.AWS_ACCESS_KEY_ID);
	debug('AWS_SECRET_ACCESS_KEY=%s', env.AWS_SECRET_ACCESS_KEY);
	debug('AWS_REGION=%s', env.AWS_REGION);
	debug('DYNAMODB_ENDPOINT=%s', env.DYNAMODB_ENDPOINT);
	debug('TABLE_PREFIX=%s', env.TABLE_PREFIX);

	U.extend(this, env);

	this.SCHEMA = U.deepFreeze({
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
});
