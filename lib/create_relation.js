'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function CreateRelation() {}

module.exports = CreateRelation;

CreateRelation.create = function createCreateRelation(spec) {
	var obj = new CreateRelation();
	return obj.initialize(spec);
};

U.extend(CreateRelation.prototype, {
	initialize: function (spec) {
		Object.defineProperties(this, {
			dynamodb: {
				value: spec.dynamodb
			}
		});

		return this;
	},

	parameters: function (subjectId, objectId) {
		return {
			Item: DynamoDB.serializeRelation({
				subject: subjectId,
				object: objectId
			}),
			TableName: this.dynamodb.relationTable(),
			ConditionExpression: 'attribute_not_exists(subject) AND ' +
				'attribute_not_exists(object)'
			// Potentially useful parameters:
			// ReturnConsumedCapacity
			// ReturnItemCollectionMetrics
		};
	},

	create: function (subjectId, objectId) {
		return this.dynamodb
			.putItem(this.parameters(subjectId, objectId))
			.then(U.constant(true));
	}
});
