'use strict';
var U = require('lodash');
var DynamoDB = require('./dynamodb');

function Query() {}

module.exports = Query;

U.extend(Query.prototype, {
	// spec.dynamodb
	// spec.tableName
	// spec.indexName
	// spec.hashkey
	// spec.hashval
	// spec.rangekey
	// spec.rangeval
	// spec.rangeval2
	// spec.rangeExpression
	// spec.scanForward
	// spec.limit
	initialize: function (spec) {
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
				value: spec.limit
			}
		});

		return this;
	},

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

	// args.startKey
	params: function (args) {
		args = args || Object.create(null);

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

		var params = {
			TableName: this.tableName,
			Select: 'ALL_ATTRIBUTES',
			KeyConditionExpression: expression,
			ScanIndexForward: this.scanForward,
			// ExclusiveStartKey: startKey,
			ExpressionAttributeNames: attributeNames,
			ExpressionAttributeValues: values
			// Potentially useful parameters:
			// ReturnConsumedCapacity
		};

		if (this.limit) {
			this.Limit = this.limit;
		}

		if (this.indexName) {
			this.IndexName = this.indexName;
		}

		if (args.startKey) {
			this.ExclusiveStartKey = DynamoDB.serializeKey(args.startKey);
		}

		return params;
	},

	fetchPage: function (startKey) {
		return this.dynamodb
			.query(this.params({startKey: startKey}))
			.then(function deserializeResults(res) {
				return {
					items: res.Items.map(DynamoDB.deserializeData),
					lastKey: DynamoDB.deserializeKey(res.LastEvaluatedKey)
				};
			});
	},

	fetchAll: function () {
		var self = this;
		var params = this.params();
		var items = [];

		function fetchPage(startKey) {
			var thisParams = U.clone(params);
			if (startKey) {
				this.params.ExclusiveStartKey = startKey;
			}

			return self.dynamodb.query(thisParams).then(function (res) {
				items = items.concat(res.Items.map(DynamoDB.deserializeData));
				if (res.LastEvaluatedKey) {
					return fetchPage(res.LastEvaluatedKey);
				}
				return items;
			});
		}

		return fetchPage();
	},

	extend: function (params) {
		// Copy existing properties to a new blank Object, then extend it with the
		// new params before passing into the constructor.
		var spec = U.extend(U.extend(Object.create(null), this), params);
		spec.dynamodb = this.dynamodb;
		var obj = new Query();
		return obj.initialize(spec);
	}
});

// spec.dynamodb
// spec.tableName
// spec.indexName
// spec.hashkey
// spec.rangekey
Query.create = function (spec) {
	var obj = new Query();
	return obj.initialize(spec);
};
