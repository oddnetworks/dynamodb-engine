'use strict';
var U = require('lodash');
var Promise = require('bluebird');
var DynamoDB = require('./dynamodb');
var errors = require('./errors');
var ThroughputExceededError = errors.get('ThroughputExceededError');

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
		var dynamodb = this.dynamodb;
		var params = this.parameters(key, type);

		function deleteItem() {
			return dynamodb
				.deleteItem(params)
				.then(U.constant(true))
				.catch(ThroughputExceededError, function () {
					return Promise.delay(2000).then(deleteItem);
				});
		}

		return deleteItem();
	}
});
