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

	parameters: function (key, type) {
		return {
			Key: DynamoDB.serializeKey(key),
			TableName: this.dynamodb.table(type),
			ReturnValues: 'NONE'
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	remove: function (key, type) {
		return this.dynamodb
			.deleteItem(this.parameters(key, type))
			.then(U.constant(true));
	}
});
