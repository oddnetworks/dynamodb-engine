'use strict';
var Promise = require('bluebird');
var debug = require('debug')('dynamodb-engine:dynamodb');
var AWS = require('aws-sdk');

var U = require('./utils');
var errors = require('./errors');

var ThroughputExceededError = errors.get('ThroughputExceededError');
var NonExistentTableError = errors.get('NonExistentTableError');
var NonExistentIndexError = errors.get('NonExistentIndexError');
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
			var bumpPollInterval = (function () {
				var current = 0;
				var count = 0;
				return function () {
					count += 1;
					if (current > (30 * 1000)) {
						return (30 * 1000);
					}
					current += (100 * count);
					return current;
				};
			})();

			function poll() {
				debug('createTable %s poll', params.TableName);
				api.describeTable({TableName: params.TableName}, function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus !== 'ACTIVE') {
						setTimeout(poll, bumpPollInterval());
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					if (indexes && indexes.length) {
						for (i; i < indexes.length; i += 1) {
							if (indexes[i].IndexStatus !== 'ACTIVE') {
								setTimeout(poll, bumpPollInterval());
								return null;
							}
						}
					}
					resolve(res.Table);
				});
			}

			debug('createTable %s', params.TableName);

			api.createTable(params, function (err) {
				debug('createTable %s callback', params.TableName);
				if (DynamoDB.isTableExistsException(err)) {
					reject(new TableExistsError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}
				poll();
			});
		});
	},

	updateTable: function (params) {
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var bumpPollInterval = (function () {
				var current = 0;
				var count = 0;
				return function () {
					count += 1;
					if (current > (30 * 1000)) {
						return (30 * 1000);
					}
					current += (100 * count);
					return current;
				};
			})();

			function poll() {
				debug('updateTable %s poll', params.TableName);
				api.describeTable({TableName: params.TableName}, function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus !== 'ACTIVE') {
						setTimeout(poll, bumpPollInterval());
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					if (indexes && indexes.length) {
						for (i; i < indexes.length; i += 1) {
							if (indexes[i].IndexStatus !== 'ACTIVE') {
								setTimeout(poll, bumpPollInterval());
								return null;
							}
						}
					}
					resolve(res.Table);
				});
			}

			debug('updateTable %s', params.TableName);

			api.updateTable(params, function (err) {
				debug('updateTable %s callback', params.TableName);
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
			var bumpPollInterval = (function () {
				var current = 0;
				var count = 0;
				return function () {
					count += 1;
					if (current > (30 * 1000)) {
						return (30 * 1000);
					}
					current += (100 * count);
					return current;
				};
			})();

			function poll() {
				debug('deleteTable %s poll', params.TableName);
				api.describeTable({TableName: params.TableName}, function (err, res) {
					if (DynamoDB.isNonExistentTableException(err)) {
						return resolve(true);
					}
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus === 'ACTIVE') {
						setTimeout(poll, bumpPollInterval());
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					if (indexes && indexes.length) {
						for (i; i < indexes.length; i += 1) {
							if (indexes[i].IndexStatus === 'ACTIVE') {
								setTimeout(poll, bumpPollInterval());
								return null;
							}
						}
					}
					resolve(res.Table);
				});
			}

			debug('deleteTable %s', params.TableName);

			api.deleteTable(params, function (err) {
				if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NotFoundError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}

				poll();
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

	batchGetItem: function (params) {
		var api = this.api;

		return new Promise(function (resolve, reject) {
			api.batchGetItem(params, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(res);
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
				} else if (DynamoDB.isNonExistentIndexException(err)) {
					reject(new NonExistentIndexError(err.message));
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

DynamoDB.deserializeItem = function deserializeItem(obj) {
	return Object.keys(obj).reduce(function (rv, key) {
		rv[key] = DynamoDB.deserializeAttribute(obj[key]);
		return rv;
	}, Object.create(null));
};

DynamoDB.deserializeAttribute = function deserializeAttribute(val) {
	if (val.hasOwnProperty('S')) {
		return val.S.toString();
	} else if (val.hasOwnProperty('N')) {
		return val.N;
	} else if (val.SS || val.NS) {
		return val.SS || val.NS;
	} else if (val.hasOwnProperty('BOOL')) {
		return Boolean(val.BOOL);
	} else if (val.hasOwnProperty('M')) {
		return DynamoDB.deserializeItem(val.M);
	} else if (val.hasOwnProperty('L')) {
		return val.L.map(DynamoDB.deserializeAttribute);
	} else if (val.hasOwnProperty('NULL')) {
		return null;
	}
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
	return {L: obj.map(DynamoDB.typeCast)};
};

DynamoDB.typeCastObject = function typeCastObject(obj) {
	var keys = Object.keys(obj);
	var rv = {M: {}};
	if (!keys.length) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
		M[key] = DynamoDB.typeCast(obj[key]);
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
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		default:
			throw new TypeError('Only String, Number or Boolean attributes (not ' +
				type + ') may be defined on keys in DynamoDB Engine.');
	}
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
		(/non-existent\stable/.test(err.message) ||
			/Table:\s([!-~]+)\snot\sfound/.test(err.message)));
};

DynamoDB.isNonExistentIndexException = function (err) {
	return (err &&
		err.code === 'ValidationException' &&
		/does not have the specified index/.test(err.message));
};
