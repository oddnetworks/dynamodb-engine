'use strict';
var EventEmitter = require('events');
var util = require('util');
var Promise = require('bluebird');
var U = require('./utils');
var DynamoDB = require('./dynamodb');

// spec.dynamodb
// spec.tableName
// spec.indexName
// spec.hashkey
// spec.rangekey
// spec.rangeval
// spec.rangeval2
// spec.rangeExpression
// spec.scanForward
// spec.limit
function Query(spec) {
	EventEmitter.init.call(this);

	Object.defineProperties(this, {
		dynamodb: {
			value: spec.dynamodb
		},
		tableName: {
			enumerable: true,
			value: spec.tableName
		},
		indexName: {
			enumerable: true,
			value: spec.indexName || null
		},
		hashkey: {
			enumerable: true,
			value: spec.hashkey
		},
		rangekey: {
			enumerable: true,
			value: spec.rangekey
		},
		hashval: {
			enumerable: true,
			value: spec.hashval
		},
		rangeval: {
			enumerable: true,
			value: spec.rangeval
		},
		rangeval2: {
			enumerable: true,
			value: spec.rangeval2
		},
		rangeExpression: {
			enumerable: true,
			value: spec.rangeExpression || null
		},
		scanForward: {
			enumerable: true,
			value: U.has(spec, 'scanForward') ? spec.scanForward : true
		},
		limit: {
			enumerable: true,
			value: spec.limit || 20
		}
	});
}

util.inherits(Query, EventEmitter);

module.exports = Query;

U.extend(Query.prototype, {
	hashEqual: function (val) {
		return this.extend({
			hashval: val
		});
	},

	rangeEqual: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: '#rangekey = :rangeval'
		});
	},

	rangeLessThan: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: '#rangekey < :rangeval'
		});
	},

	rangeLessThanOrEqual: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: '#rangekey <= :rangeval'
		});
	},

	rangeGreaterThan: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: '#rangekey > :rangeval'
		});
	},

	rangeGreaterThanOrEqual: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: '#rangekey >= :rangeval'
		});
	},

	rangeBetween: function (a, b) {
		return this.extend({
			rangeval: a,
			rangeval2: b,
			rangeExpression: '#rangekey BETWEEN :rangeval AND :rangeval2'
		});
	},

	rangeBeginsWith: function (val) {
		return this.extend({
			rangeval: val,
			rangeExpression: 'begins_with(#rangekey, :rangeval)'
		});
	},

	ascending: function () {
		return this.extend({
			scanForward: true
		});
	},

	descending: function () {
		return this.extend({
			scanForward: false
		});
	},

	limit: function (val) {
		return this.extend({
			limit: val
		});
	},

	// params.debug
	fetch: function (params) {
		params = params || Object.create(null);

		var attributeNames = {
			'#hashkey': this.hashkey
		};
		var values = {
			':hashval': DynamoDB.valueDefinition(this.hashval)
		};

		if (!U.isUndefined(this.rangeval)) {
			attributeNames['#rangekey'] = this.rangekey;
			values[':rangeval'] = DynamoDB.valueDefinition(this.rangeval);
		}
		if (!U.isUndefined(this.rangeval2)) {
			values[':rangeval2'] = DynamoDB.valueDefinition(this.rangeval2);
		}

		var expression = '#hashkey = :hashval';
		if (this.rangeExpression) {
			expression += (' AND ' + this.rangeExpression);
		}

		var newParams = {
			TableName: this.tableName,
			Select: 'ALL_ATTRIBUTES',
			KeyConditionExpression: expression,
			ScanIndexForward: this.scanForward,
			Limit: this.limit,
			// ExclusiveStartKey: startKey,
			ExpressionAttributeNames: attributeNames,
			ExpressionAttributeValues: values,
			ReturnConsumedCapacity: params.debug ? 'TOTAL' : 'NONE'
		};

		if (this.indexName) {
			newParams.IndexName = this.indexName;
		}

		var self = this;

		return new Promise(function (resolve, reject) {
			return self.dynamodb.query(newParams, function (err, res) {
				if (err) {
					return reject(err);
				}
				resolve(res);
			});
		});
	},

	extend: function (params) {
		// Copy existing properties to a new blank Object, then extend it with the
		// new params before passing into the constructor.
		var spec = U.extend(U.extend(Object.create(null), this), params);
		spec.dynamodb = this.dynamodb;
		return new Query(spec);
	}
});

// spec.dynamodb
// spec.tableName
// spec.indexName
// spec.hashkey
// spec.rangekey
Query.create = function (spec) {
	return new Query(spec);
};
