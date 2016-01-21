'use strict';
var Promise = require('bluebird');
var U = require('./utils');
var debug = require('debug')('dynamodb-engine:main');
var errors = require('./errors');
var DynamoDB = require('./dynamodb');
var TableSchema = require('./table_schema');
var Query = require('./query');

var ConditionFailedError = errors.get('ConditionFailedError');

var API = Object.create(null);

// options.accessKeyId
// options.secretAccessKey
// options.region
// options.endpoint
// options.tablePrefix
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

	var dynamodb = DynamoDB.create({
		accessKeyId: accessKeyId,
		secretAccessKey: secretAccessKey,
		region: region,
		endpoint: endpoint,
		tablePrefix: tablePrefix
	});

	Object.defineProperties(this, {
		initialized: {
			value: true
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

		spec.throughput = {
			read: 10,
			write: 5
		};

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

				indexSpec.throughput = {
					read: 10,
					write: 5
				};

				return indexSpec;
			});
		}

		definitions[spec.tableName] = U.deepFreeze(spec);
		return definitions;
	}, Object.create(null));

	var relationTable = {
		tableName: dynamodb.relationTable(),
		attributes: {
			subject: 'String',
			predicate: 'String',
			object: 'String'
		},
		keys: {
			hash: 'subject',
			range: 'object'
		},
		throughput: {
			read: 10,
			write: 5
		},
		indexes: [
			{
				indexName: dynamodb.hasManyIndex(),
				keys: {
					hash: 'subject',
					range: 'predicate'
				},
				projection: 'ALL',
				throughput: {
					read: 10,
					write: 5
				}
			},
			{
				indexName: dynamodb.belongsToIndex(),
				keys: {
					hash: 'object',
					range: 'predicate'
				},
				projection: 'ALL',
				throughput: {
					read: 10,
					write: 5
				}
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
		return dynamodb.describeTable({TableName: tableName})
			.then(function (res) {
				debug('%s already exists', tableName);
				return res.Table;
			})
			.catch(self.NotFoundError, function () {
				debug('%s needs to be created', tableName);
				var params = TableSchema
					.createFromDefinition(self.schema[tableName])
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
	record = record || Object.create(null);
	if (!record.id || !U.isString(record.id)) {
		throw new Error('#createRecord(record) record.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!record.type || !U.isString(record.type)) {
		throw new Error('#createRecord(record) record.type is a ' +
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
	record = record || Object.create(null);
	if (!record.id || !U.isString(record.id)) {
		throw new Error('#updateRecord(record) record.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!record.type || !U.isString(record.type)) {
		throw new Error('#updateRecord(record) record.type is a ' +
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

	debug('update record %s', record.id);
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
		throw new Error('#getRecord(type, id) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!id || !U.isString(type)) {
		throw new Error('#getRecord(type, id) id is a ' +
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

	debug('get record %s %s', type, id);
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
		throw new Error('#removeRecord(type, id) type is a ' +
			'required String in DynamoDB Engine');
	}
	if (!id || !U.isString(type)) {
		throw new Error('#removeRecord(type, id) id is a ' +
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

	debug('remove record %s %s', type, id);
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

API.createRelation = function createRelation(subjectId, object) {
	object = object || Object.create(null);
	if (!subjectId || !U.isString(subjectId)) {
		throw new Error('#createRelation(subjectId, object) subjectId is a ' +
			'required String in DynamoDB Engine');
	}
	if (!object.id || !U.isString(object.id)) {
		throw new Error('#createRelation(subjectId, object) object.id is a ' +
			'required String in DynamoDB Engine');
	}
	if (!object.type || !U.isString(object.type)) {
		throw new Error('#createRelation(subjectId, object) object.type is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		Item: DynamoDB.serializeRelation({
			subjectId: subjectId,
			object: object
		}),
		TableName: this.dynamodb.relationTable()
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	var self = this;

	debug('create relation %s %s %s', subjectId, object.type, object.id);
	return this.dynamodb
		.putItem(params)
		.then(function () {
			debug('created relation %s %s %s', subjectId, object.type, object.id);
			return object;
		})
		.catch(this.NonExistentTableError, function () {
			debug('created relation %s %s -- missing table', subjectId, object.type);
			var msg = 'Entity table for relations does not exist.';
			msg += ' A migration is probably required.';
			return Promise.reject(new self.OperationalError(msg));
		});
};

API.getRelations = function getRelations(subjectId, predicate) {
	if (!subjectId || !U.isString(subjectId)) {
		throw new Error('#getRelations(subjectId, predicate) subjectId is a ' +
			'required String in DynamoDB Engine');
	}
	if (!predicate || !U.isString(predicate)) {
		throw new Error('#getRelation(subjectId, predicate) predicate is a ' +
			'required String in DynamoDB Engine');
	}

	var query = Query.create({
		dynamodb: this.dynamodb,
		tableName: this.dynamodb.relationTable(),
		indexName: this.dynamodb.hasManyIndex(),
		hashkey: 'subject',
		hashval: subjectId,
		rangekey: 'predicate'
	});

	debug('get relations %s %s', subjectId, predicate);
	return query.rangeEqual(predicate).fetchAll().then(function (items) {
		debug('got relations %s %s', subjectId, predicate);
		return items
			.map(DynamoDB.deserializeItem)
			.map(function (item) {
				return item.object;
			});
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
		throw new Error('dynamodb-engine .create(options, schema): ' +
			'schema must be an Object');
	}
	Object.keys(schema).forEach(function (type) {
		var table = schema[type];
		if (table.indexes) {
			Object.keys(table.indexes).forEach(function (name) {
				var index = table.indexes[name];
				if (!index.keys || !U.isObject(index.keys)) {
					throw new Error('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys must be an Object');
				}
				if (!index.keys.hash || !U.isObject(index.keys.hash)) {
					throw new Error('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash must be an Object');
				}
				var hash = index.keys.hash;
				if (!hash.name || !U.isString(hash.name)) {
					throw new Error('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.name must be a valid String');
				}
				if (!hash.type || !U.isString(hash.type)) {
					throw new Error('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.type must be a valid String');
				}
				if ('String Number Boolean'.indexOf(hash.type) === -1) {
					throw new Error('dynamodb-engine .create(options, schema): ' +
						'schema[type].indexes[name].keys.hash.type must be ' +
						'"String", "Number" or "Boolean"');
				}
				if (index.keys.range) {
					var range = index.keys.range;
					if (!U.isObject(range)) {
						throw new Error('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range must be an Object');
					}
					if (!range.name || !U.isString(range.name)) {
						throw new Error('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range.name must be a valid String');
					}
					if (!hash.type || !U.isString(range.type)) {
						throw new Error('dynamodb-engine .create(options, schema): ' +
							'schema[type].indexes[name].keys.range.type must be a valid String');
					}
					if ('String Number Boolean'.indexOf(range.type) === -1) {
						throw new Error('dynamodb-engine .create(options, schema): ' +
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
