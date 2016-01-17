'use strict';
var U = require('lodash');
var Promise = require('bluebird');
var DynamoDB = require('./dynamodb');
var errors = require('./errors');

var ConditionFailedError = errors.get('ConditionFailedError');
var RecordExistsError = errors.get('RecordExistsError');
var NotFoundError = errors.get('NotFoundError');
var TableNotActiveError = errors.get('TableNotActiveError');

var API = Object.create(null);

// options:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
API.initialize = function () {
	if (this.initialized) {
		return this;
	}

	var dynamodb = this.dynamodb;
	Object.keys(this.schema).reduce(function (schema, type) {
		var tableName = dynamodb.table(type);
		schema[tableName] = schema[type];
		schema[tableName].tableName = tableName;
		delete schema[type];
		return schema;
	}, this.schema);

	var relationTableSchema = createRelationTableSchema(dynamodb);
	this.schema[relationTableSchema.tableName] = relationTableSchema;

	Object.defineProperies(this, {
		initialized: {
			value: true
		}
	});

	return this;
};

API.migrateUp = function migrateUp() {
	var self = this;

	Object.keys(this.schema).map(function (tableName) {
		self.describeTable(tableName).then(function (Table) {
			checkTableSchema(self.schema[tableName], Table);
		});
	});
};

API.describeTable = function describeTable(tableName) {
	if (!tableName || !U.isString(tableName)) {
		throw new Error('#describeTable(tableName) tableName is a ' +
			'required String in DynamoDB Engine');
	}

	var params = {
		TableName: tableName
	};

	return this.dynamodb.describeTable(params).then(function (res) {
		return res.Table;
	});
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

	return this.dynamodb
		.putItem(params)
		.then(U.constant(record))
		.catch(ConditionFailedError, function () {
			return Promise.reject(new RecordExistsError(record.id));
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
		TableName: this.dynamodb.table(record.type),
		ConditionExpression: 'attribute_exists(id)'
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	return this.dynamodb
		.putItem(params)
		.then(U.constant(record))
		.catch(ConditionFailedError, function () {
			return Promise.reject(new NotFoundError(record.id));
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

	return this.dynamodb
		.putItem(params)
		.then(U.constant(object));
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
		TableName: this.dynamodb.table(type),
		// Projecting no attributes will fetch them all.
		ProjectionExpression: ''
		// Potentially useful parameters:
		// ReturnConsumedCapacity
		// ReturnItemCollectionMetrics
	};

	return this.dynamodb
		.getItem(params)
		.then(function deserializeData(res) {
			return DynamoDB.deserializeData(res.Item);
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

	return this.dynamodb
		.deleteItem(params)
		.then(U.constant(true));
};

exports.API = API;

// options:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
exports.create = function (options) {
	var api = Object.create(API);
	return api.initialize(options);
};

function createRelationTableSchema(dynamodb) {
	return {
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
		globalIndexes: [
			{
				indexName: dynamodb.hasManyIndex(),
				keys: {
					hash: 'subject',
					range: 'predicate'
				}
			},
			{
				indexName: dynamodb.belongsToIndex(),
				keys: {
					hash: 'object',
					range: 'predicate'
				}
			}
		]
	};
}

function checkTableSchema(schema, Table) {
	if (Table.TableStatus !== 'ACTIVE') {
		throw new TableNotActiveError(Table.TableName);
	}

	// Check for HASH and RANGE key
	// var KeySchema = checkKeySchema(schema.keys, Table.KeySchema);

	// Check Table.GlobalSecondaryIndexes
	var Indexes = schema.globalIndexes.reduce(function (Indexes, index) {
		var GSI = U.find(Table.GlobalSecondaryIndexes, {IndexName: index.indexName});

		// Check that the index definition exists.
		if (!GSI) {
			Indexes.push({Create: {
				IndexName: index.indexName,
				KeySchema: DynamoDB.keySchema(index.keys),
				Projection: {ProjectionType: 'ALL'},
				ProvisionedThroughput: {
					ReadCapacityUnits: 10,
					WriteCapacityUnits: 5
				}
			}});
			return Indexes;
		}

		if (GSI.IndexStatus !== 'ACTIVE') {
			throw new TableNotActiveError(GSI.IndexName);
		}

		// Check that the KeySchema is valid
		var KeySchema = checkKeySchema(index.keys, GSI.KeySchema);

		if (KeySchema.length) {
			Indexes.push({Delete: {IndexName: GSI.IndexName}});
			Indexes.push({Create: {
				IndexName: index.indexName,
				KeySchema: KeySchema,
				Projection: GSI.ProjectionType,
				ProvisionedThroughput: GSI.ProvisionedThroughput
			}});
		}
	}, []);

	return Indexes;
}

function checkKeySchema(spec, current) {
	return Object.keys(spec).reduce(function (KeySchema, type) {
		var attributeName = spec[type];
		type = type.toUpperCase();

		function createDefinition() {
			KeySchema.push({
				AttributeName: attributeName,
				KeyType: type
			});
			return KeySchema;
		}

		// If we are already making a change to KeySchema, then do it for
		// both HASH key and RANGE key.
		if (KeySchema.length) {
			return createDefinition();
		}

		var def = U.find(current, {KeyType: type});
		// If the key is not defined or the AttributeName has changed,
		// then define it.
		if (!def || def.AttributeName !== attributeName) {
			return createDefinition();
		}

		return KeySchema;
	}, []);
}
