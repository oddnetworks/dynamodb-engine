'use strict';
var EventEmitter = require('events');
var util = require('util');
var Promise = require('bluebird');
var U = require('./utils');
var DynamoDB = require('./dynamodb');
var constants = require('./constants');
var errors = require('./errors');
var ThroughputExceededError = errors.ThroughputExceededError;
var DynamoDBError = errors.DynamoDBError;
var NotFoundError = errors.NotFoundError;
var NonExistentTableError = errors.NonExistentTableError;

var LOGLEVELS_WARN = constants.get('LOGLEVELS_WARN');
var LOGLEVELS_INFO = constants.get('LOGLEVELS_INFO');
var LOGLEVELS_DEBUG = constants.get('LOGLEVELS_DEBUG');

// spec.tableName
// spec.dynamodb
// spec.idAttribute
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
		idAttribute: {
			enumerable: true,
			value: spec.idAttribute || 'id'
		}
	});
}

util.inherits(Table, EventEmitter);

module.exports = Table;

U.extend(Table.prototype, {

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

	put: function (attributes) {
		if (!attributes) {
			throw new Error('Table#put(attributes) must have ' +
				'an attributes parameter');
		}
		if (!U.isObject(attributes) || Array.isArray(attributes)) {
			throw new Error('Table#put(attributes) attributes must be ' +
				'an Object.');
		}

		var self = this;

		var params = {
			Item: DynamoDB.serializeRecord(attributes),
			TableName: this.tableName
		};

		return new Promise(function (resolve, reject) {
			self.dynamodb.putItem(params, function (err) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(attributes);
			});
		});
	},

	remove: function (id, options) {
		if (!id) {
			throw new Error('table.remove(record) requires a record.id String');
		}
		if (!U.isString(id)) {
			throw new Error('table.remove(record) record.id must be a String');
		}

		options = options || Object.create(null);
		var self = this;

		var idAttribute = options.idAttribute || this.idAttribute;

		var key = {};
		key[idAttribute] = {
			S: id
		};

		var params = {
			Key: key,
			TableName: this.tableName
		};

		return new Promise(function (resolve, reject) {
			self.emit('log', {
				level: LOGLEVELS_DEBUG,
				message: 'attempting to DELETE ' + id + ' from ' + self.tableName
			});
			self.dynamodb.deleteItem(params, function (err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					self.emit('log', {
						level: LOGLEVELS_WARN,
						message: 'ProvisionedThroughputExceededException in ' +
							'DELETE ' + id + ' from ' + self.tableName,
						error: err
					});
					return reject(new ThroughputExceededError(err.message));
				}
				if (err) {
					self.emit('log', {
						level: LOGLEVELS_WARN,
						message: 'Unexpected exception in ' +
							'DELETE ' + id + ' from ' + self.tableName,
						error: err
					});
					return reject(err);
				}
				if (res) {
					self.emit('log', {
						level: LOGLEVELS_INFO,
						message: 'DELETE ' + id + ' from ' + self.tableName
					});
					return resolve(true);
				}
				self.emit('log', {
					level: LOGLEVELS_INFO,
					message: 'DELETE ' + id + ' from ' + self.tableName + ' not found'
				});
				reject(new NotFoundError('Could not find entity by id ' + id));
			});
		});
	},

	// keys - *Array* of Strings.
	// options.idAttribute - *String* (default = this.idAttribute)
	// options.ConsistentRead - *Boolean* (default = false)
	// options.ReturnConsumedCapacity - *String* 'INDEXES | TOTAL | NONE' (default = 'NONE')
	getBatch: function (keys, options) {
		options = options || Object.create(null);
		var self = this;
		var RequestItems = {};

		var idAttribute = options.idAttribute || this.idAttribute;

		RequestItems[this.tableName] = {
			Keys: keys.map(function (key) {
				var dbKey = {};
				dbKey[idAttribute] = {S: key};
				return dbKey;
			}),
			ConsistentRead: Boolean(options.ConsistentRead)
		};

		var params = {
			RequestItems: RequestItems,
			ReturnConsumedCapacity: options.ReturnConsumedCapacity || 'NONE'
		};

		return new Promise(function (resolve, reject) {
			self.emit('log', {
				level: LOGLEVELS_DEBUG,
				message: 'attempting to batch GET from ' + keys[0] + ' from ' + self.tableName
			});
			self.dynamodb.batchGetItem(params, function (err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					self.emit('log', {
						level: LOGLEVELS_WARN,
						message: 'ProvisionedThroughputExceededException in ' +
							'batch GET from ' + keys[0] + ' from ' + self.tableName,
						error: err
					});
					return reject(new ThroughputExceededError(err.message));
				}
				if (err) {
					self.emit('log', {
						level: LOGLEVELS_WARN,
						message: 'Unexpected exception in batch GET from ' +
							keys[0] + ' from ' + self.tableName,
						error: err
					});
					return reject(err);
				}
				self.emit('log', {
					level: LOGLEVELS_INFO,
					message: 'batch GET from ' + keys[0] + ' from ' + self.tableName
				});
				resolve(res);
			});
		});
	}
});

Table.create = function createTable(tableName, dynamodb) {
	return new Table({
		tableName: tableName,
		dynamodb: dynamodb
	});
};

