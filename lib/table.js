'use strict';
var EventEmitter = require('events');
var util = require('util');
var Promise = require('bluebird');
var U = require('./utils');
var DynamoDB = require('./dynamodb');
var Query = require('./query');
var errors = require('./errors');
var ThroughputExceededError = errors.ThroughputExceededError;
var DynamoDBError = errors.DynamoDBError;
var NotFoundError = errors.NotFoundError;
var NonExistentTableError = errors.NonExistentTableError;
var ConditionFailedError = errors.ConditionFailedError;

// spec.tableName
// spec.dynamodb
// spec.hashkey
// spec.rangekey
function Table(spec) {
	EventEmitter.init.call(this);

	Object.defineProperties(this, {
		tableName: {
			enumerable: true,
			value: spec.tableName
		},
		dynamodb: {
			enumerable: true,
			value: spec.dynamodb
		},
		hashkey: {
			enumerable: true,
			value: spec.hashkey
		},
		rangekey: {
			enumerable: true,
			value: spec.rangekey
		}
	});
}

util.inherits(Table, EventEmitter);

module.exports = Table;

U.extend(Table.prototype, {

	// key - Object representing the primary key to get
	get: function (key) {
		var primaryKey = Object.keys(key).reduce(function (primaryKey, k) {
			switch (typeof key[k]) {
				case 'string':
					primaryKey[k] = {S: key[k]};
					break;
				case 'number':
					primaryKey[k] = {N: key[k]};
					break;
				case 'boolean':
					primaryKey[k] = {BOOLEAN: key[k]};
					break;
				default:
					throw new Error('Only String, Number, or Boolean keys ' +
						'are supported in DynamoDB Engine');
			}
			return primaryKey;
		}, {});

		var params = {
			Key: primaryKey,
			TableName: this.tableName,
			// Projecting no attributes will fetch them all.
			ProjectionExpression: ''
		};

		var self = this;

		return new Promise(function (resolve, reject) {
			self.dynamodb.getItem(params, function (err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (res) {
					return resolve(DynamoDB.deserializeData(res.Item));
				}
				reject(new NotFoundError('Could not find entity'));
			});
		});
	},

	put: function (attributes, options) {
		if (!attributes) {
			throw new Error('Table#put(attributes) must have ' +
				'an attributes parameter');
		}
		if (!U.isObject(attributes) || Array.isArray(attributes)) {
			throw new Error('Table#put(attributes) attributes must be ' +
				'an Object.');
		}

		options = options || Object.create(null);
		var self = this;

		var params = {
			Item: DynamoDB.serializeRecord(attributes),
			TableName: this.tableName
		};

		if (options.condition) {
			params.ConditionExpression = options.condition;
		}

		return new Promise(function (resolve, reject) {
			self.dynamodb.putItem(params, function (err) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (DynamoDB.isConditionalFailedException(err)) {
					reject(new ConditionFailedError(err.message));
				} else if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(attributes);
			});
		});
	},

	// params.indexName
	query: function (params) {
		params = params || Object.create(null);
		return Query.create({
			dynamodb: this.dynamodb,
			tableName: this.tableName,
			indexName: params.indexName,
			hashkey: this.hashkey,
			rangekey: this.rangekey
		});
	}
});

// spec.tableName
// spec.dynamodb
// spec.hashkey
// spec.rangekey
Table.create = function createTable(spec) {
	return new Table({
		tableName: spec.tableName,
		dynamodb: spec.dynamodb,
		hashkey: spec.hashkey,
		rangekey: spec.rangekey
	});
};

