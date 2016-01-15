'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function UpdateRecord() {}

module.exports = UpdateRecord;

UpdateRecord.create = function createUpdateRecord(spec) {
	var obj = new UpdateRecord();
	return obj.initialize(spec);
};

U.extend(UpdateRecord.prototype, {
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
			Item: DynamoDB.serializeRecord(record),
			TableName: this.dynamodb.table(record.type)
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	update: function (record) {
		return this.dynamodb.putItem(this.parameters(record));
	}
});
