'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function GetRecord() {}

module.exports = GetRecord;

GetRecord.create = function createGetRecord(spec) {
	var obj = new GetRecord();
	return obj.initialize(spec);
};

U.extend(GetRecord.prototype, {
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
			// Projecting no attributes will fetch them all.
			ProjectionExpression: ''
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	get: function (key, type) {
		return this.dynamodb
			.getItem(this.parameters(key, type))
			.then(function deserializeData(res) {
				return DynamoDB.deserializeData(res.Item);
			});
	}
});
