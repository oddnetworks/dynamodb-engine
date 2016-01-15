'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function RemoveRecord() {}

module.exports = RemoveRecord;

RemoveRecord.create = function createRemoveRecord(spec) {
	var obj = new RemoveRecord();
	return obj.initialize(spec);
};

U.extend(RemoveRecord.prototype, {
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
			Key: DynamoDB.primaryKey({id: record.id}),
			TableName: this.dynamodb.table(record.type),
			ReturnValues: 'NONE'
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	remove: function (record) {
		return this.dynamodb.deleteItem(this.parameters(record));
	}
});
