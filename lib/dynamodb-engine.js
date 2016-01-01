'use strict';
var EventEmitter = require('events');
var util = require('util');
var Promise = require('bluebird');
var AWS = require('aws-sdk');
var uuid = require('node-uuid');
var utils = require('./utils');
var errors = require('./errors');
var NotFoundError = errors.NotFoundError;

var LOGLEVELS_WARN = 'WARN';

var STREAM_TYPES = [
	'KEYS_ONLY',
	'OLD_IMAGE',
	'NEW_AND_OLD_IMAGES',
	'NEW_IMAGE'
];

function DynamoDBEngine() {}

util.inherits(DynamoDBEngine, EventEmitter);

module.exports = DynamoDBEngine;

utils.extend(DynamoDBEngine.prototype, {
	// spec.accessKeyId - *String*
	// spec.secretAccessKey = *String*
	// spec.region - *String*
	// spec.tableName - *String* name of the table to use.
	// spec.endpoint - *String* HTTP endpoint to use (optional).
	// spec.idAttribute - *String* name of the key ID attribute used on models.
	initialize: function (spec) {
		EventEmitter.init.call(this);
		this.idAttribute = spec.idAttribute || 'id';
		this.tableName = spec.tableName;
		var awsOptions = {
			accessKeyId: spec.accessKeyId,
			secretAccessKey: spec.secretAccessKey,
			region: spec.region,
			apiVersion: '2012-08-10'
		};
		if (spec.endpoint) {
			awsOptions.endpoint = spec.endpoint;
		}
		this.dynamodb = new AWS.DynamoDB(awsOptions);
	},

	get: function (record, options) {
		var id = record.id;
		if (!id) {
			throw new Error('dynamoDBEngine.get(record) requires a record.id String');
		}
		if (typeof id !== 'string') {
			throw new Error('dynamoDBEngine.get(record) record.id must be a String');
		}

		options = options || Object.create(null);
		var self = this;

		var idAttribute = options.idAttribute || this.idAttribute;

		var tableName = options.tableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		var key = {};
		key[idAttribute] = {
			S: id
		};

		var params = {
			Key: key,
			TableName: tableName,
			// Projecting no attributes will fetch them all.
			ProjectionExpression: ''
		};

		return new Promise(function (resolve, reject) {
			self.dynamodb.getItem(params, function (err, data) {
				if (err) {
					return reject(err);
				}
				if (data) {
					return resolve({
						id: id,
						data: DynamoDBEngine.deserializeData(data.Item)
					});
				}
				reject(new NotFoundError('Could not find entity by id ' + id));
			});
		});
	},

	post: function (record, options) {
		if (record.id) {
			throw new Error('dynamoDBEngine.post(record) must not have a ' +
											'record.id attribute.');
		}
		var data = record.data;
		if (!data) {
			throw new Error('dynamoDBEngine.post(record) must have a record.data ' +
											'attribute.');
		}
		if (typeof data !== 'object') {
			throw new Error('dynamoDBEngine.post(record) record.data must be an ' +
											'Object.');
		}

		options = options || Object.create(null);
		var self = this;

		var idAttribute = options.idAttribute || this.idAttribute;

		var tableName = options.tableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		var newRecord = {
			id: DynamoDBEngine.uuid(),
			data: data
		};

		var params = {
			Item: DynamoDBEngine.serializeRecord(newRecord, {
				idAttribute: idAttribute
			}),
			TableName: tableName,
			ConditionExpression: 'attribute_not_exists(' + idAttribute + ')'
		};

		return new Promise(function (resolve, reject) {
			self.dynamodb.putItem(params, function (err) {
				if (err) {
					return reject(err);
				}
				resolve(newRecord);
			});
		});
	},

	put: function (record, options) {
		var id = record.id;
		if (!id) {
			throw new Error('dynamoDBEngine.put(record) requires a record.id String');
		}
		if (typeof id !== 'string') {
			throw new Error('dynamoDBEngine.put(record) record.id must be a String');
		}
		var data = record.data;
		if (!data) {
			throw new Error('dynamoDBEngine.put(record) must have a record.data ' +
											'attribute.');
		}
		if (typeof data !== 'object') {
			throw new Error('dynamoDBEngine.put(record) record.data must be an ' +
											'Object.');
		}

		options = options || Object.create(null);
		var self = this;

		var idAttribute = options.idAttribute || this.idAttribute;

		var tableName = options.tableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		var newRecord = {
			id: id,
			data: data
		};

		var params = {
			Item: DynamoDBEngine.serializeRecord(newRecord, {
				idAttribute: idAttribute
			}),
			TableName: tableName
		};

		return new Promise(function (resolve, reject) {
			self.dynamodb.putItem(params, function (err) {
				if (err) {
					return reject(err);
				}
				resolve(newRecord);
			});
		});
	},

	remove: function (record, options) {
		var id = record.id;
		if (!id) {
			throw new Error('dynamoDBEngine.remove(record) requires a record.id String');
		}
		if (typeof id !== 'string') {
			throw new Error('dynamoDBEngine.remove(record) record.id must be a String');
		}

		options = options || Object.create(null);
		var self = this;

		var idAttribute = options.idAttribute || this.idAttribute;

		var tableName = options.tableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		var key = {};
		key[idAttribute] = {
			S: id
		};

		var params = {
			Key: key,
			TableName: tableName
		};

		return new Promise(function (resolve, reject) {
			self.dynamodb.deleteItem(params, function (err, data) {
				if (err) {
					return reject(err);
				}
				if (data) {
					return resolve(true);
				}
				reject(new NotFoundError('Could not find entity by id ' + id));
			});
		});
	},

	// keys - *Array* of Strings.
	// options.tableName - *String* (default = this.tableName)
	// options.idAttribute - *String* (default = this.idAttribute)
	// options.ConsistentRead - *Boolean* (default = false)
	// options.ReturnConsumedCapacity - *String* 'INDEXES | TOTAL | NONE' (default = 'NONE')
	getBatch: function (keys, options) {
		options = options || Object.create(null);
		var self = this;
		var RequestItems = {};

		var tableName = options.tableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		var idAttribute = options.idAttribute || this.idAttribute;

		RequestItems[tableName] = {
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
			self.dynamodb.batchGetItem(params, function (err, data) {
				if (err) {
					return reject(err);
				}
				resolve(data);
			});
		});
	},

	// Generic method of creating a DynamoDB table
	// AWS Docs: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property
	//
	// Example params:
	//
	// var params = {
	//   tableName: 'STRING_VALUE', /* default = this.tableName */
	//   keys: { /* required */
	//     hash: { /* required if there is not a unique range key */
	//       name: 'STRING_VALUE' /* required */
	//     },
	//     range: {
	//       name: 'STRING_VALUE', /* required */
	//       type: 'String | Number | Boolean' /* required */
	//     }
	//   },
	//   throughput: {
	//     read: 10, /* default = 10 */
	//     write: 5 /* default = 5 */
	//   },
	//   stream: 'NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY'
	// };
	//
	createGenericTable: function (params) {
		var self = this;

		var tableName = params.tableName || params.TableName || this.tableName;
		if (!tableName) {
			throw new Error('This dynamoDBEngine instance does not have an ' +
											'associated tableName.');
		}

		params.keys = params.keys || Object.create(null);
		var attributes = [];
		var keys = [];
		var rangeKeyType;

		if (!params.keys.hash && !params.keys.range) {
			throw new Error('dynamoDBEngine.createTable(params) expects ' +
				'params.keys.hash and/or params.keys.range');
		}
		if (params.keys.hash) {
			if (!params.keys.hash.name || typeof params.keys.hash.name !== 'string') {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.keys.hash.name to be a String.');
			}
			if (params.keys.hash.type && params.keys.hash.type !== 'String') {
				throw new Error('dynamoDBEngine.createTable(params) only supports ' +
					'params.keys.hash.type as "String".');
			}
			attributes.push({
				AttributeName: params.keys.hash.name,
				// Only support primary hash key as a String.
				AttributeType: 'S'
			});
			keys.push({
				AttributeName: params.keys.hash.name,
				KeyType: 'HASH'
			});
		}
		if (params.keys.range) {
			if (!params.keys.range.name || typeof params.keys.range.name !== 'string') {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.keys.range.name to be a String.');
			}
			if (params.keys.range.type !== 'String') {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.keys.range.type to be a String.');
			}
			rangeKeyType = params.keys.range.type.slice(0, 1).toUpperCase();
			if (['S', 'N', 'B'].indexOf(rangeKeyType) === -1) {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.keys.range.type to be "String | Number | Boolean".');
			}
			attributes.push({
				AttributeName: params.keys.range.name,
				AttributeType: rangeKeyType
			});
			keys.push({
				AttributeName: params.keys.range.name,
				KeyType: 'RANGE'
			});
		}

		var throughput = params.throughput || params.ProvisionedThroughput || {};

		var stream = params.stream || params.StreamSpecification;

		var newParams = {
			TableName: tableName,
			AttributeDefinitions: attributes,
			KeySchema: keys,
			ProvisionedThroughput: {
				ReadCapacityUnits: throughput.read || throughput.ReadCapacityUnits || 10,
				WriteCapacityUnits: throughput.write || throughput.WriteCapacityUnits || 5
			}
		};

		if (stream) {
			if (STREAM_TYPES.indexOf(stream) === -1) {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.stream to be "' + STREAM_TYPES.join(' | ') + '"');
			}
			newParams.StreamSpecification = {
				StreamEnabled: true,
				StreamViewType: stream
			};
		}

		return new Promise(function (resolve, reject) {
			self.dynamodb.waitFor(
				'tableExists',
				{TableName: newParams.TableName},
				function (err, res) {
					if (err) {
						return reject(err);
					}
					resolve(res.Table);
				});

			self.dynamodb.createTable(newParams, function (err) {
				if (err) {
					if (err.name === 'ResourceInUseException' &&
						/already exists/.test(err.message)) {
						self.emit('log', {
							level: LOGLEVELS_WARN,
							message: 'attempting to create table, table "' +
								newParams.TableName + '" already exists'
						})
					} else {
						reject(err);
					}
				}
			});
		});
	},

	// A shortcut for creating a simple key value table for
	// this DynamoDBEngine instance.
	//
	// Example params:
	//
	// var params = {
	//   throughput: {
	//     read: 10, /* default = 10 */
	//     write: 5 /* default = 5 */
	//   },
	//   stream: 'NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY'
	// };
	//
	createTable: function (params) {
		var newParams = utils.extend({}, params);

		newParams.keys = {
			hash: {
				name: this.idAttribute,
				type: 'String'
			}
		};

		return this.createGenericTable(newParams);
	},

	// params.tableName - *String* optional (default = this.tableName)
	deleteTable: function (params) {
		params = params || {};

		params.TableName =
			params.tableName ||
			params.TableName ||
			this.tableName;
		delete params.tableName;

		var self = this;
		return new Promise(function (resolve, reject) {
			self.dynamodb.waitFor(
				'tableNotExists',
				{TableName: params.TableName},
				function (err, res) {
					if (err) {
						return reject(err);
					}
					resolve(res);
				});
			self.dynamodb.deleteTable(params, function (err) {
				if (err) {
					reject(err);
				}
			});
		});
	},

	// Returns a Promise for a list of tables (Array of Strings) available to
	// the credentials in this DynamoDBEngine instance.
	listTables: function () {
		var self = this;
		return new Promise(function (resolve, reject) {
			self.dynamodb.listTables({}, function (err, res) {
				if (err) {
					return reject(err);
				}
				resolve(res.TableNames);
			});
		});
	}
});

// record.id
// record.data
// options.idAttribute
DynamoDBEngine.serializeRecord = function (record, options) {
	var idAttribute = options.idAttribute;
	var dynamoRecord = {};
	dynamoRecord[idAttribute] = {
		S: record.id
	};

	Object.keys(record.data).reduce(function (rec, key) {
		var val;
		if (key !== idAttribute) {
			rec[key] = DynamoDBEngine.typeCast(record.data[key]);
			if (val) {
				rec[key] = val;
			}
		}
		return rec;
	}, dynamoRecord);
};

DynamoDBEngine.typeCast = function (obj) {
	switch (typeof obj) {
		case 'string':
			return {S: obj};
		case 'number':
			if (isNaN(obj)) {
				throw new TypeError('Cannot set NaN as a number on DynamoDB.');
			}
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		case 'function':
			throw new TypeError('Cannot set a function value on DynamoDB.');
		case 'undefined':
			break;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ?
				DynamoDBEngine.typeCastArray(obj) :
				DynamoDBEngine.typeCastObject(obj);
	}
};

DynamoDBEngine.typeCastArray = function (obj) {
	if (!obj.length) {
		return {NULL: true};
	}

	var rv = {};
	var type = typeof obj[0];
	var key;

	// We only accept String Lists or Number Lists.
	if (type === 'string') {
		key = 'SS';
	} else if (type === 'number') {
		key = 'NS';
	} else {
		throw new TypeError('Only String or Number Lists (not ' + type +
												') may be defined in DynamoDB Engine.');
	}

	rv[key] = obj.map(function (val) {
		if (typeof val !== type) {
			throw new TypeError('All values in a String or Number List must be of ' +
													'the same type in DynamoDB Engine.');
		}
		return val;
	});

	return rv;
};

DynamoDBEngine.typeCastObject = function (obj) {
	var keys = Object.keys(obj);
	var rv = {M: {}};
	if (!keys.length) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
		var type = typeof obj[key];
		var val = obj[key];
		switch (type) {
			case 'string':
				M[key] = {S: val};
				break;
			case 'number':
				M[key] = {N: val};
				break;
			case 'boolean':
				M[key] = {BOOL: val};
				break;
			default:
				throw new TypeError('Only String, Number or Boolean attributes ' +
														'(not ' + type + ') may be defined on Mixed ' +
														'Objects in DynamoDB Engine.');
		}
		return M;
	}, rv.M);

	return rv;
};

DynamoDBEngine.deserializeData = function (obj) {
	return Object.keys(obj).reduce(function (rv, key) {
		var val = obj[key];
		if (val.hasOwnProperty('S')) {
			rv[key] = val.S.toString();
		} else if (val.hasOwnProperty('N')) {
			rv[key] = val.N;
		} else if (val.SS || val.NS) {
			rv[key] = val.SS || val.NS;
		} else if (val.hasOwnProperty('BOOL')) {
			rv[key] = Boolean(val.BOOL);
		} else if (val.hasOwnProperty('M')) {
			rv[key] = DynamoDBEngine.deserializeData(val.M);
		} else if (val.hasOwnProperty('NULL')) {
			rv[key] = null;
		}
		return rv;
	}, Object.create(null));
};

DynamoDBEngine.uuid = function () {
	// A version 4 UUID is a random number generated UUID, rather than being
	// tied to the MAC address and a timestamp like version 1 UUIDs.
	return uuid.v4({rng: uuid.nodeRNG});
};

DynamoDBEngine.create = function (spec) {
	var engine = new DynamoDBEngine();
	engine.initialize(spec);
	return engine;
};
