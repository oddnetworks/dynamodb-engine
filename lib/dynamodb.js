'use strict';
var EventEmitter = require('events');
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

var hasOwn = Object.prototype.hasOwnProperty;

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
			},
			log: {
				enumerable: true,
				value: new EventEmitter()
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

	emitRequest: function (apiCall, req) {
		var self = this;
		self.log.emit('request', {
			apiCall: apiCall,
			operation: req.operation,
			method: req.httpRequest.method,
			href: req.httpRequest.endpoint.href,
			body: req.httpRequest.body
		});
	},

	listTables: function (params) {
		params = params || {};
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.listTables(params, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(res);
			});

			self.emitRequest('listTables', req);
		});
	},

	describeTable: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.describeTable(params, function (err, res) {
				if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NotFoundError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}
				resolve(res);
			});

			self.emitRequest('describeTable', req);
		});
	},

	createTable: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var bumpPollInterval = createPollInterval(50, 30000);

			function poll() {
				debug('createTable %s poll', params.TableName);
				var req = api.describeTable({TableName: params.TableName}, function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus !== 'ACTIVE') {
						setTimeout(poll, bumpPollInterval());
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					if (indexes && indexes.length > 0) {
						for (i; i < indexes.length; i += 1) {
							if (indexes[i].IndexStatus !== 'ACTIVE') {
								setTimeout(poll, bumpPollInterval());
								return null;
							}
						}
					}
					resolve(res.Table);
				});

				self.emitRequest('createTable poll', req);
			}

			debug('createTable %s', params.TableName);

			var req = api.createTable(params, function (err) {
				debug('createTable %s callback', params.TableName);
				if (DynamoDB.isTableExistsException(err)) {
					reject(new TableExistsError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}
				poll();
			});

			self.emitRequest('createTable', req);
		});
	},

	updateTable: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var bumpPollInterval = createPollInterval(500, 30000);

			function poll() {
				debug('updateTable %s poll', params.TableName);
				var req = api.describeTable({TableName: params.TableName}, function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					if (res.Table.TableStatus !== 'ACTIVE') {
						debug('updateTable %s %s', params.TableName, res.Table.TableStatus);
						setTimeout(poll, bumpPollInterval());
						return null;
					}
					var indexes = res.Table.GlobalSecondaryIndexes;
					var i = 0;
					if (indexes && indexes.length > 0) {
						for (i; i < indexes.length; i += 1) {
							if (indexes[i].IndexStatus !== 'ACTIVE') {
								debug(
									'updateTable %s index %s %s',
									params.TableName,
									indexes[i].IndexName,
									indexes[i].IndexStatus
								);
								setTimeout(poll, bumpPollInterval());
								return null;
							}
						}
					}
					resolve(res.Table);
				});

				self.emitRequest('updateTable poll', req);
			}

			debug('updateTable %s', params.TableName);

			var req = api.updateTable(params, function (err) {
				debug('updateTable %s callback', params.TableName);
				if (err) {
					return reject(new DynamoDBError(err));
				}
				poll();
			});

			self.emitRequest('updateTable', req);
		});
	},

	deleteTable: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var bumpPollInterval = createPollInterval(50, 30000);

			function poll() {
				debug('deleteTable %s poll', params.TableName);
				var req = api.describeTable({TableName: params.TableName}, function (err) {
					if (DynamoDB.isNonExistentTableException(err)) {
						return resolve(true);
					}
					if (err) {
						return reject(new DynamoDBError(err));
					}

					setTimeout(poll, bumpPollInterval());
				});

				self.emitRequest('describeTable', req);
			}

			debug('deleteTable %s', params.TableName);

			var req = api.deleteTable(params, function (err) {
				if (DynamoDB.isNonExistentTableException(err)) {
					reject(new NotFoundError(err.message));
				} else if (err) {
					reject(new DynamoDBError(err));
				}

				poll();
			});

			self.emitRequest('deleteTable', req);
		});
	},

	getItem: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.getItem(params, function getItemCallback(err, res) {
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

			self.emitRequest('getItem', req);
		});
	},

	batchGetItem: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.batchGetItem(params, function (err, res) {
				if (err) {
					return reject(new DynamoDBError(err));
				}
				resolve(res);
			});

			self.emitRequest('batchGetItem', req);
		});
	},

	putItem: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.putItem(params, function putItemCallback(err) {
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

			self.emitRequest('putItem', req);
		});
	},

	deleteItem: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.deleteItem(params, function deleteItemCallback(err, res) {
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

			self.emitRequest('deleteItem', req);
		});
	},

	query: function (params) {
		var self = this;
		var api = this.api;

		return new Promise(function (resolve, reject) {
			var req = api.query(params, function getItemCallback(err, res) {
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

			self.emitRequest('query', req);
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
	if (hasOwn.call(val, 'S')) {
		return val.S.toString();
	} else if (hasOwn.call(val, 'N')) {
		return parseFloat(val.N);
	} else if (val.SS || val.NS) {
		return val.SS || val.NS;
	} else if (hasOwn.call(val, 'BOOL')) {
		return Boolean(val.BOOL);
	} else if (hasOwn.call(val, 'M')) {
		return DynamoDB.deserializeItem(val.M);
	} else if (hasOwn.call(val, 'L')) {
		return val.L.map(DynamoDB.deserializeAttribute);
	} else if (hasOwn.call(val, 'NULL')) {
		return null;
	}
};

DynamoDB.typeCast = function typeCast(obj) {
	switch (typeof obj) {
		case 'string':
			if (obj.length === 0) {
				return null;
			}
			return {S: obj};
		case 'number':
			if (isNaN(obj)) {
				return null;
			}
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		case 'function':
		case 'undefined':
			return null;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ? DynamoDB.typeCastArray(obj) : DynamoDB.typeCastObject(obj);
	}
};

DynamoDB.typeCastArray = function typeCastArray(obj) {
	return {L: obj.map(DynamoDB.typeCast)};
};

DynamoDB.typeCastObject = function typeCastObject(obj) {
	var keys = Object.keys(obj);
	var rv = {M: {}};
	if (keys.length === 0) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
		var val = DynamoDB.typeCast(obj[key]);
		if (val) {
			M[key] = val;
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
	if (!err) {
		return false;
	}

	return (err.message === 'Requested resource not found' ||
		(err.code === 'ResourceNotFoundException' && (/non-existent\stable/.test(err.message) ||
			/Table:\s([!-~]+)\snot\sfound/.test(err.message))));
};

DynamoDB.isNonExistentIndexException = function (err) {
	return (err &&
		err.code === 'ValidationException' &&
		/does not have the specified index/.test(err.message));
};

function createPollInterval(interval, limit) {
	var current = 0;
	var count = 0;
	return function () {
		count += 1;
		if (current > limit) {
			return limit;
		}
		current += (interval * count);
		return current;
	};
}
