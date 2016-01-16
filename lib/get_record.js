'use strict';
var U = require('lodash');
var Promise = require('bluebird');
var DynamoDB = require('./dynamodb');
var errors = require('./errors');
var ThroughputExceededError = errors.get('ThroughputExceededError');

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
		var dynamodb = this.dynamodb;
		var params = this.parameters(key, type);

		function getItem() {
			return dynamodb
				.getItem(params)
				.then(function deserializeData(res) {
					return DynamoDB.deserializeData(res.Item);
				})
				.catch(ThroughputExceededError, function () {
					return Promise.delay(2000).then(getItem);
				});
		}

		return getItem();
	}
});
