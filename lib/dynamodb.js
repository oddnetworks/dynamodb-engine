'use strict';
var Promise = require('bluebird');
var U = require('./utils');
var AWS = require('aws-sdk');
var errors = require('./errors');
var ThroughputExceededError = errors.get('ThroughputExceededError');
var NonExistentTableError = errors.get('NonExistentTableError');
var TableExistsError = errors.get('TableExistsError');
var ConditionFailedError = errors.get('ConditionFailedError');
var DynamoDBError = errors.get('DynamoDBError');
var NotFoundError = errors.get('NotFoundError');

function DynamoDB() {}

module.exports = DynamoDB;

U.extend(DynamoDB.prototype, {
	// spec.accessKeyId
	// spec.secretAccessKey
	// spec.region
	// spec.endpoint
	// spec.tablePrefix
	initialize: function (spec) {
		Object.defineProperties(this, {
			api: {
				enumerable: true,
				value: DynamoDB.createAWSDynamoDB(spec)
			},
			tablePrefix: {
				enumerable: true,
				value: spec.tablePrefix
			}
		});
		return this;
	},

	table: function (type) {
		return U.snakeCase(this.tablePrefix + '_' + type + '_entities');
	},

	index: function (type, name) {
		return U.snakeCase(this.tablePrefix + '_' + type + '_' + name);
	},

	relationTable: function () {
		return U.snakeCase(this.tablePrefix + '_relations');
	},

	hasManyIndex: function () {
		return U.snakeCase(this.tablePrefix + '_has_many');
	},

	belongsToIndex: function () {
		return U.snakeCase(this.tablePrefix + '_belongs_to');
	},

	listTables: function (params) {
		params = params || {};
		var api = this.api;

		return new Promise(function (resolve, reject) {
			api.listTables(params, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(res);
			});
		});
	},

	describeTable: function (params) {
		var api = this.api;

		return new Promise(function (resolve, reject) {
			api.describeTable(params, function (err, res) {
				if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NotFoundError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}
				resolve(res);
			});
		});
	},

	createTable: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			var inFlight = false;
			var returnValue;

			var waitFor = {TableName: params.TableName};
			api.waitFor('tableExists', waitFor, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				if (inFlight) {
					resolve(res);
				} else {
					returnValue = res;
				}
			});

			api.createTable(params, function (err) {
				inFlight = true;
				if (DynamoDB.isTableExistsException(err)) {
					reject(new TableExistsError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (returnValue) {
					resolve(returnValue);
				}
			});
		});
	},

	updateTable: function (params) {
		var api = this.api;

		return new Promise(function (resolve, reject) {
			function poll() {
				api.describeTable({TableName: params.TableName}, function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus !== 'ACTIVE') {
						setTimeout(poll, 4000);
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					for (i; i < indexes.length; i += 1) {
						if (indexes[i].IndexStatus !== 'ACTIVE') {
							setTimeout(poll, 4000);
							return null;
						}
					}
					resolve(res.Table);
				});
			}

			api.updateTable(params, function (err) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				poll();
			});
		});
	},

	deleteTable: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			var inFlight = false;
			var returnValue;

			var waitFor = {TableName: params.TableName};
			api.waitFor('tableNotExists', waitFor, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				if (inFlight) {
					resolve(res);
				} else {
					returnValue = res;
				}
			});

			api.deleteTable(params, function (err) {
				inFlight = true;
				if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NotFoundError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (returnValue) {
					resolve(returnValue);
				}
			});
		});
	},

	getItem: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			api.getItem(params, function getItemCallback(err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (res) {
					return resolve(res);
				}
				reject(new NotFoundError('Could not find entity'));
			});
		});
	},

	putItem: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			api.putItem(params, function putItemCallback(err) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (DynamoDB.isConditionalFailedException(err)) {
					reject(new ConditionFailedError(err.message));
				} else if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(true);
			});
		});
	},

	deleteItem: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			api.deleteItem(params, function deleteItemCallback(err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (res) {
					return resolve(res);
				}
				reject(new NotFoundError('Could not find entity'));
			});
		});
	},

	query: function (params) {
		var api = this.api;
		return new Promise(function (resolve, reject) {
			api.query(params, function getItemCallback(err, res) {
				if (DynamoDB.isProvisionedThroughputExceededException(err)) {
					reject(new ThroughputExceededError(err.message));
				} else if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NonExistentTableError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}
				resolve(res);
			});
		});
	}
});

// spec.accessKeyId
// spec.secretAccessKey
// spec.region
// spec.endpoint
// spec.tablePrefix
DynamoDB.create = function (spec) {
	var obj = new DynamoDB();
	return obj.initialize(spec);
};

// spec.accessKeyId
// spec.secretAccessKey
// spec.region
// spec.endpoint
DynamoDB.createAWSDynamoDB = function createAWSDynamoDB(options) {
	var awsOptions = {
		accessKeyId: options.accessKeyId,
		secretAccessKey: options.secretAccessKey,
		region: options.region,
		apiVersion: options.apiVersion || '2012-08-10'
	};
	if (options.endpoint) {
		awsOptions.endpoint = options.endpoint;
	}

	return new AWS.DynamoDB(awsOptions);
};

DynamoDB.serializeItem = function serializeItem(attrs) {
	return Object.keys(attrs).reduce(function (rec, key) {
		var val = DynamoDB.typeCast(attrs[key]);
		if (val) {
			rec[key] = val;
		}
		return rec;
	}, {});
};

DynamoDB.serializeRelation = function serializeRelation(args) {
	return DynamoDB.serializeItem({
		subject: args.subjectId,
		predicate: args.object.type,
		object: args.object
	});
};

DynamoDB.deserializeItem = function deserializeItem(obj) {
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
			rv[key] = DynamoDB.deserializeItem(val.M);
		} else if (val.hasOwnProperty('NULL')) {
			rv[key] = null;
		}
		return rv;
	}, Object.create(null));
};

DynamoDB.typeCast = function typeCast(obj) {
	switch (typeof obj) {
		case 'string':
			if (obj.length === 0) {
				throw new TypeError('Cannot set empty string attributes on DynamoDB.');
			}
			return {S: obj};
		case 'number':
			if (isNaN(obj)) {
				throw new TypeError('Cannot set NaN as an attribute on DynamoDB.');
			}
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		case 'function':
			throw new TypeError('Cannot set a function as an attribute on DynamoDB.');
		case 'undefined':
			break;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ?
				DynamoDB.typeCastArray(obj) :
				DynamoDB.typeCastObject(obj);
	}
};

DynamoDB.typeCastArray = function typeCastArray(obj) {
	if (!obj.length) {
		return {NULL: true};
	}

	var rv = {};
	var type = typeof obj[0];
	var key;

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

DynamoDB.typeCastObject = function typeCastObject(obj) {
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
				throw new TypeError('Only String, Number or Boolean attributes (not ' +
					type + ') may be defined on Mixed Objects in DynamoDB Engine.');
		}
		return M;
	}, rv.M);

	return rv;
};

DynamoDB.serializeKey = function serializeKey(obj) {
	return Object.keys(obj).reduce(function (keys, key) {
		keys[key] = DynamoDB.typeCastKey(obj[key]);
		return keys;
	}, {});
};

DynamoDB.typeCastKey = function typeCastKey(obj) {
	var type = typeof obj;
	switch (type) {
		case 'string':
			return {S: obj};
		case 'number':
			return {N: obj};
		case 'boolean':
			return {B: obj};
		default:
			throw new TypeError('Only String, Number or Boolean attributes (not ' +
				type + ') may be defined on keys in DynamoDB Engine.');
	}
};

DynamoDB.deserializeKey = function deserializeKey(obj) {
	return Object.keys(obj).reduce(function (rv, key) {
		var val = obj[key];
		if (U.has(val, 'S')) {
			rv[key] = val.S.toString();
		} else if (U.has(val, 'N')) {
			rv[key] = val.N;
		} else if (U.has(val, 'B')) {
			rv[key] = Boolean(val.B);
		}
		return rv;
	}, Object.create(null));
};

DynamoDB.keySchema = function keySchema(keys) {
	keys = keys || Object.create(null);
	var keySchema = [];

	keySchema.push({
		AttributeName: keys.hash,
		KeyType: 'HASH'
	});

	if (keys.range) {
		keySchema.push({
			AttributeName: keys.range,
			KeyType: 'RANGE'
		});
	}

	return keySchema;
};

DynamoDB.isResourceNotFoundException = function (err) {
	return err && err.code === 'ResourceNotFoundException';
};

DynamoDB.isConditionalFailedException = function (err) {
	return (err && err.code === 'ConditionalCheckFailedException');
};

DynamoDB.isProvisionedThroughputExceededException = function (err) {
	return err && err.code === 'ProvisionedThroughputExceededException';
};

DynamoDB.isTableExistsException = function (err) {
	return (err &&
		err.code === 'ResourceInUseException' &&
		/already\sexists/.test(err.message));
};

DynamoDB.isNonExistentTableException = function (err) {
	return (err &&
		err.code === 'ResourceNotFoundException' &&
		/non-existent\stable/.test(err.message));
};
