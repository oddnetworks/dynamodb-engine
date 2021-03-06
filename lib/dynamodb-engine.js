'use strict';
var EventEmitter = require('events');
var Promise = require('bluebird');
var debug = require('debug')('dynamodb-engine:main');

var U = require('./utils');
var errors = require('./errors');
var DynamoDB = require('./dynamodb');
var TableSchema = require('./table-schema');
var Query = require('./query');

var ConditionFailedError = errors.get('ConditionFailedError');

var API = Object.create(null);

// options.accessKeyId
// options.secretAccessKey
// options.region
// options.endpoint
// options.tablePrefix
// options.defaultThroughput
API.initialize = function (options) {
	if (this.initialized) {
		return this;
	}

	options = options || Object.create(null);
	var accessKeyId = options.accessKeyId;
	var secretAccessKey = options.secretAccessKey;
	var region = options.region;
	var endpoint = options.endpoint;
	var tablePrefix = options.tablePrefix;
	var defaultThroughput = options.defaultThroughput || {read: 5, write: 1};

	debug(
		'initializing with tablePrefix %s and endpoint %s',
		tablePrefix,
		endpoint
	);

	var log = new EventEmitter();

	var dynamodb = DynamoDB.create({
		accessKeyId: accessKeyId,
		secretAccessKey: secretAccessKey,
		region: region,
		endpoint: endpoint,
		tablePrefix: tablePrefix
	});

	dynamodb.log.on('request', function (ev) {
		log.emit('request', ev);
	});

	Object.defineProperties(this, {
		initialized: {
			value: true
		},
		log: {
			enumerable: true,
			value: log
		},
		accessKeyId: {
			enumerable: true,
			value: accessKeyId
		},
		region: {
			enumerable: true,
			value: region
		},
		endpoint: {
			enumerable: true,
			value: endpoint || null
		},
		tablePrefix: {
			enumerable: true,
			value: tablePrefix
		},
		defaultThroughput: {
			enumerable: true,
			value: defaultThroughput
		},
		dynamodb: {
			enumerable: true,
			value: dynamodb
		},
		OperationalError: {
			enumerable: true,
			value: errors.get('OperationalError')
		},
		DynamoDBError: {
			enumerable: true,
			value: errors.get('DynamoDBError')
		},
		NotFoundError: {
			enumerable: true,
			value: errors.get('NotFoundError')
		},
		ConditionFailedError: {
			enumerable: true,
			value: errors.get('ConditionFailedError')
		},
		ThroughputExceededError: {
			enumerable: true,
			value: errors.get('ThroughputExceededError')
		},
		NonExistentTableError: {
			enumerable: true,
			value: errors.get('NonExistentTableError')
		},
		NonExistentIndexError: {
			enumerable: true,
			value: errors.get('NonExistentIndexError')
		},
		TableExistsError: {
			enumerable: true,
			value: errors.get('TableExistsError')
		},
		TableNotActiveError: {
			enumerable: true,
			value: errors.get('TableNotActiveError')
		},
		ConflictError: {
			enumerable: true,
			value: errors.get('ConflictError')
		}
	});

	return this;
};

API.defineSchema = function defineSchema(schema) {
	var self = this;
	var dynamodb = this.dynamodb;

	var definitions = Object.keys(schema).reduce(function (definitions, type) {
		var table = schema[type];
		var spec = Object.create(null);

		spec.tableName = dynamodb.table(type);

		spec.attributes = {
			id: 'String'
		};

		spec.keys = {
			hash: 'id'
		};

		spec.throughput = U.merge({}, self.defaultThroughput, table.throughput);

		if (table.indexes) {
			spec.indexes = Object.keys(table.indexes).map(function (name) {
				var index = table.indexes[name];
				var indexSpec = Object.create(null);

				indexSpec.indexName = dynamodb.index(type, name);

				indexSpec.keys = Object.keys(index.keys).reduce(function (keys, keyType) {
					var keyDef = index.keys[keyType];
					keys[keyType] = keyDef.name;
					if (!spec.attributes[keyDef.name]) {
						spec.attributes[keyDef.name] = keyDef.type;
					}
					return keys;
				}, Object.create(null));

				indexSpec.projection = 'ALL';

				indexSpec.throughput = U.merge({}, self.defaultThroughput, index.throughput);

				return indexSpec;
			});
		}

		definitions[spec.tableName] = U.deepFreeze(spec);
		return definitions;
	}, Object.create(null));

	var relationTable = {
		tableName: dynamodb.relationTable(),
		attributes: {
			subjectId: 'String',
			subjectType: 'String',
			objectId: 'String',
			objectType: 'String'
		},
		keys: {
			hash: 'subjectId',
			range: 'objectId'
		},
		throughput: U.merge({}, self.defaultThroughput),
		indexes: [
			{
				indexName: dynamodb.hasManyIndex(),
				keys: {
					hash: 'subjectId',
					range: 'objectType'
				},
				projection: 'ALL',
				throughput: U.merge({}, self.defaultThroughput)
			},
			{
				indexName: dynamodb.belongsToIndex(),
				keys: {
					hash: 'objectId',
					range: 'subjectType'
				},
				projection: 'ALL',
				throughput: U.merge({}, self.defaultThroughput)
			}
		]
	};

	definitions[relationTable.tableName] = relationTable;

	Object.defineProperty(this, 'schema', {
		enumerable: true,
		value: U.deepFreeze(definitions)
	});

	return this;
};

API.migrateUp = function migrateUp() {
	var self = this;
	var dynamodb = this.dynamodb;
	debug('begin migrateUp');

	var promises = Object.keys(this.schema).map(function (tableName) {
		var spec = self.schema[tableName];

		return dynamodb.describeTable({TableName: tableName})
			.then(function (res) {
				if (res.Table.TableStatus !== 'ACTIVE') {
					return Promise.reject(new self.TableNotActiveError(res.Table.TableName));
				}

				debug('%s already exists', tableName);
				if (spec.indexes && spec.indexes.length > 0) {
					var updateDelta = TableSchema
						.createFromDefinition(spec)
						.updateDelta(TableSchema.create(res.Table));
					if (updateDelta) {
						debug(
							'%s requires an index update for %s',
							tableName,
							updateDelta.GlobalSecondaryIndexUpdates.map(function (spec) {
								return spec.Create.IndexName;
							})
						);
						return dynamodb.updateTable(updateDelta).then(function (res) {
							debug('%s index update complete', tableName);
							return res;
						});
					}
				}
				return res.Table;
			})
			.catch(self.NotFoundError, function () {
				debug('%s needs to be created', tableName);
				var params = TableSchema
					.createFromDefinition(spec)
					.createTableParams();
				return dynamodb.createTable(params).then(function (res) {
					debug('%s created', tableName);
					return res.Table;
				});
			});
	});

	return Promise.all(promises);
};

API.createRecord = function createRecord(record) {
	record = U.cloneDeep(U.ensure(record));
	if (!record.id || !U.isString(record.id)) {
		throw new TypeError('#createRecord(record) record.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!record.type || !U.isString(record.type)) {
		throw new TypeError('#createRecord(record) record.type is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Item: DynamoDB.serializeItem(record),
		TableName: this.dynamodb.table(record.type),
		ConditionExpression: 'attribute_not_exists(id)'
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('creating record %s', record.id);
	return this.dynamodb
		.putItem(params)
		.then(function () {
			debug('created record %s', record.id);
			return record;
		})
		.catch(ConditionFailedError, function () {
			debug('creating record -- record exists %s', record.id);
			return Promise.reject(new self.ConflictError(record.id));
		})
		.catch(this.NonExistentTableError, function () {
			debug('creating record %s -- missing table', record.id);
			var msg = 'Entity table for ' + record.type + ' does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.updateRecord = function updateRecord(record) {
	record = U.cloneDeep(U.ensure(record));
	if (!record.id || !U.isString(record.id)) {
		throw new TypeError('#updateRecord(record) record.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!record.type || !U.isString(record.type)) {
		throw new TypeError('#updateRecord(record) record.type is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Item: DynamoDB.serializeItem(record),
		TableName: this.dynamodb.table(record.type)
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('updating record %s', record.id);
	return this.dynamodb
		.putItem(params)
		.then(function () {
			debug('updated record %s', record.id);
			return record;
		})
		.catch(this.NonExistentTableError, function () {
			debug('update record %s -- missing table', record.id);
			var msg = 'Entity table for ' + record.type + ' does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.getRecord = function getRecord(type, id) {
	if (!type || !U.isString(type)) {
		throw new TypeError('#getRecord(type, id) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!id || !U.isString(type)) {
		throw new TypeError('#getRecord(type, id) id is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Key: DynamoDB.serializeKey({id: id}),
		TableName: this.dynamodb.table(type)
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('getting record %s %s', type, id);
	return this.dynamodb
		.getItem(params)
		.then(function deserializeData(res) {
			debug('got record %s %s', type, id);
			if (res.Item) {
				return DynamoDB.deserializeItem(res.Item);
			}
			return Promise.reject(new self.NotFoundError('Could not find record'));
		})
		.catch(this.NotFoundError, function (err) {
			debug('get record %s %s -- not found', type, id);
			return Promise.reject(err);
		})
		.catch(this.NonExistentTableError, function () {
			debug('get record %s %s -- missing table', type, id);
			var msg = 'Entity table for ' + type + ' does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.removeRecord = function removeRecord(type, id) {
	if (!type || !U.isString(type)) {
		throw new TypeError('#removeRecord(type, id) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!id || !U.isString(type)) {
		throw new TypeError('#removeRecord(type, id) id is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Key: DynamoDB.serializeKey({id: id}),
		TableName: this.dynamodb.table(type),
		ReturnValues: 'NONE'
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('removing record %s %s', type, id);
	return this.dynamodb
		.deleteItem(params)
		.then(function () {
			debug('removed record %s %s', type, id);
			return true;
		})
		.catch(this.NonExistentTableError, function () {
			debug('remove record %s %s -- missing table', type, id);
			var msg = 'Entity table for ' + type + ' does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.createRelation = function createRelation(subject, object) {
	subject = U.ensure(subject);
	object = U.ensure(object);

	if (!U.isFullString(subject.id)) {
		throw new TypeError('#createRelation(subject, object) subject.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!U.isFullString(subject.type)) {
		throw new TypeError('#createRelation(subject, object) subject.type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!U.isFullString(object.id)) {
		throw new TypeError('#createRelation(subject, object) object.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!U.isFullString(object.type)) {
		throw new TypeError('#createRelation(subject, object) object.type is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Item: DynamoDB.serializeItem({
			subjectId: subject.id,
			subjectType: subject.type,
			objectId: object.id,
			objectType: object.type
		}),
		TableName: this.dynamodb.relationTable()
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('creating relation %s %s %s %s', subject.type, subject.id, object.type, object.id);
	return this.dynamodb
		.putItem(params)
		.then(function () {
			debug('created relation %s %s %s %s', subject.type, subject.id, object.type, object.id);
			return object;
		})
		.catch(this.NonExistentTableError, function () {
			debug('creating relation failed -- missing table');
			var msg = 'Relations table does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.getRelations = function getRelations(subjectId, predicate) {
	if (!U.isFullString(subjectId)) {
		throw new TypeError('#getRelations(subjectId, predicate) subjectId is a ' +
			'required String in DynamoDB Engine');
	}

	var query = Query.create({
		dynamodb: this.dynamodb,
		tableName: this.dynamodb.relationTable(),
		indexName: this.dynamodb.hasManyIndex(),
		hashkey: 'subjectId',
		hashval: subjectId,
		rangekey: 'objectType'
	});

	if (U.isFullString(predicate)) {
		query = query.rangeEqual(predicate);
	}

	var self = this;

	debug('getting relations %s predicate %s', subjectId, predicate || 'none');
	return query.fetchAll()
		.then(function (items) {
			debug('got relations %s predicate %s', subjectId, predicate || 'none');
			return items
				.map(function (item) {
					return {
						id: item.objectId,
						type: item.objectType
					};
				});
		})
		.catch(this.OperationalError, function (err) {
			var msg;
			if (/^Table ([\w]+) does not exist/.test(err.message)) {
				debug('getting relations failed -- missing table');
				msg = 'Relations table does not exist.';
				msg += ' A migration is probably required.';
				return Promise.reject(new self.OperationalError(msg));
			}
			if (/^Index ([\w]+) does not exist/.test(err.message)) {
				debug('getting relations failed -- missing index');
				msg = 'Index for relations does not exist.';
				msg += ' A migration is probably required.';
				return Promise.reject(new self.OperationalError(msg));
			}
			return Promise.reject(err);
		});
};

API.getReverseRelations = function getReverseRelations(objectId, predicate) {
	if (!U.isFullString(objectId)) {
		throw new TypeError('#getReverseRelations(objectId, predicate) objectId is a ' +
			'required String in DynamoDB Engine');
	}

	var query = Query.create({
		dynamodb: this.dynamodb,
		tableName: this.dynamodb.relationTable(),
		indexName: this.dynamodb.belongsToIndex(),
		hashkey: 'objectId',
		hashval: objectId,
		rangekey: 'subjectType'
	});

	if (U.isFullString(predicate)) {
		query = query.rangeEqual(predicate);
	}

	var self = this;

	debug('getting reverse relations %s predicate %s', objectId, predicate || 'none');
	return query.fetchAll()
		.then(function (items) {
			debug('got reverse relations %s predicate %s', objectId, predicate || 'none');
			return items
				.map(function (item) {
					return {
						id: item.subjectId,
						type: item.subjectType
					};
				});
		})
		.catch(this.OperationalError, function (err) {
			var msg;
			if (/^Table ([\w]+) does not exist/.test(err.message)) {
				debug('getting reverse relations failed -- missing table');
				msg = 'Relations table does not exist.';
				msg += ' A migration is probably required.';
				return Promise.reject(new self.OperationalError(msg));
			}
			if (/^Index ([\w]+) does not exist/.test(err.message)) {
				debug('getting reverse relations failed -- missing index');
				msg = 'Index for relations does not exist.';
				msg += ' A migration is probably required.';
				return Promise.reject(new self.OperationalError(msg));
			}
			return Promise.reject(err);
		});
};

API.removeRelation = function removeRelation(subjectId, objectId) {
	if (!subjectId || !U.isString(subjectId)) {
		throw new TypeError('#removeRelation(subjectId, object) subjectId is a ' +
			'required String in DynamoDB Engine');
	}
	if (!objectId || !U.isString(objectId)) {
		throw new TypeError('#removeRelation(subjectId, object) object.id is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Key: DynamoDB.serializeKey({subjectId: subjectId, objectId: objectId}),
		TableName: this.dynamodb.relationTable(),
		ReturnValues: 'NONE'
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('removing relation %s %s', subjectId, objectId);
	return this.dynamodb
		.deleteItem(params)
		.then(function () {
			debug('removed relation %s %s', subjectId, objectId);
			return true;
		})
		.catch(this.NonExistentTableError, function () {
			debug('remove relation failed -- missing table');
			var msg = 'Relations table does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.query = function query(type, index) {
	if (!type || !U.isString(type)) {
		throw new TypeError('#query(args) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!index || !U.isString(index)) {
		throw new TypeError('#query(args) index is a ' +
			'required String in DynamoDB Engine');
	}

	var msg;
	var tableName = this.dynamodb.table(type);
	var table = this.schema[tableName];
	if (!table) {
		msg = 'No Table is defined in the schema for ' + type;
		throw new this.OperationalError(msg);
	}
	var indexes = table.indexes;
	if (!indexes || indexes.length === 0) {
		msg = 'No indexes are defined in the schema for ' + type;
		throw new this.OperationalError(msg);
	}
	var indexName = this.dynamodb.index(type, index);
	var indexSpec = U.find(indexes, {indexName: indexName});
	if (!indexSpec) {
		msg = 'Index ' + index + ' not defined in the schema for ' + type;
		throw new this.OperationalError(msg);
	}

	return Query.create({
		dynamodb: this.dynamodb,
		tableName: tableName,
		indexName: indexName,
		hashkey: indexSpec.keys.hash,
		rangekey: indexSpec.keys.range
	});
};

API.batchGet = function batchGet(type, ids) {
	var self = this;

	if (!type || !U.isString(type)) {
		throw new TypeError('#batchGet(type, ids) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!ids || !Array.isArray(ids)) {
		throw new TypeError('#batchGet(type, ids) ids is a ' +
			'required Array in DynamoDB Engine');
	}

	var tableName = this.dynamodb.table(type);
	var RequestItems = {};
	RequestItems[tableName] = {
		Keys: ids.map(function (id) {
			return DynamoDB.serializeKey({id: id});
		})
	};

	var params = {
		RequestItems: RequestItems
	};

	return this.dynamodb.batchGetItem(params)
		.then(function deserializeData(res) {
			return res.Responses[tableName].map(function (item) {
				return DynamoDB.deserializeItem(item);
			});
		})
		.catch(this.NonExistentTableError, function () {
			// debug('get record %s %s -- missing table', type, id);
			var msg = 'Entity table for ' + type + ' does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

exports.API = API;

// options.accessKeyId
// options.secretAccessKey
// options.region
// options.endpoint
// options.tablePrefix
exports.create = function (options, schema) {
	if (!schema || !U.isObject(schema)) {
		throw new TypeError('dynamodb-engine .create(options, schema): ' +
			'schema must be an Object');
	}
	Object.keys(schema).forEach(function (type) {
		var table = schema[type];
		if (table.indexes) {
			Object.keys(table.indexes).forEach(function (name) {
				var index = table.indexes[name];
				if (!index.keys || !U.isObject(index.keys)) {
					throw new TypeError('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys must be an Object');
				}
				if (!index.keys.hash || !U.isObject(index.keys.hash)) {
					throw new TypeError('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash must be an Object');
				}
				var hash = index.keys.hash;
				if (!hash.name || !U.isString(hash.name)) {
					throw new TypeError('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.name must be a valid String');
				}
				if (!hash.type || !U.isString(hash.type)) {
					throw new TypeError('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.type must be a valid String');
				}
				if ('String Number Boolean'.indexOf(hash.type) === -1) {
					throw new TypeError('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.type must be ' +
						'"String", "Number" or "Boolean"');
				}
				if (index.keys.range) {
					var range = index.keys.range;
					if (!U.isObject(range)) {
						throw new TypeError('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range must be an Object');
					}
					if (!range.name || !U.isString(range.name)) {
						throw new TypeError('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range.name must be a valid String');
					}
					if (!hash.type || !U.isString(range.type)) {
						throw new TypeError('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range.type must be a valid String');
					}
					if ('String Number Boolean'.indexOf(range.type) === -1) {
						throw new TypeError('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range.type must be ' +
							'"String", "Number" or "Boolean"');
					}
				}
			});
		}
	});

	var api = Object.create(API);

	return api
		.initialize(options)
		.defineSchema(schema);
};
