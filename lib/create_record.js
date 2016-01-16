'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function CreateRecord() {}

module.exports = CreateRecord;

CreateRecord.create = function createCreateRecord(spec) {
	var obj = new CreateRecord();
	return obj.initialize(spec);
};

U.extend(CreateRecord.prototype, {
	initialize: function (spec) {
		Object.defineProperties(this, {
			dynamodb: {
				value: spec.dynamodb
			}
		});

		return this;
	},

	parameters: function (record) {
		return {
			Item: DynamoDB.serializeItem(record),
			TableName: this.dynamodb.table(record.type),
			ConditionExpression: 'attribute_not_exists(id)'
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	create: function (record) {
		return this.dynamodb
			.putItem(this.parameters(record))
			.then(U.constant(true));
	}
});
