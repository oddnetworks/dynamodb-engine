'use strict';
var U = require('./utils');
var DynamoDB = require('./dynamodb');

function BatchGet() {}

module.exports = BatchGet;

U.extend(BatchGet.prototype, {
	initialize: function (spec) {
		Object.defineProperties(this, {
			dynamodb: {
				value: spec.dynamodb
			}
		});

		return this;
	},

	// query - Object
	//   query[table] - Array
	//     query[table][i] - Object
	//       query[table][i][key] - String or Number
	query: function (query) {
		return Object.keys(query).reduce(function (items, table) {
			var keys = query[table];
			return items.concat(keys.map(function (key) {
				return {
					table: table,
					key: key
				};
			}));
		}, []);
	},

	// DynamoDB only allows us to get items 100 at a time, so we have to
	// jump through some hoops to follow the rulz.
	//
	// query - Object
	//   query[type] - Array
	//     query[type][i] - Object
	//       query[type][i][key] - String or Number
	fetchAll: function (query) {
		var dynamodb = this.dynamodb;
		var results = Object.create(null);

		function fetchPage(items) {
			var chunk = items.slice(0, 100);
			var rest = items.slice(100, items.length);

			var RequestItems = chunk.reduce(function (items, spec) {
				if (!items[spec.table]) {
					items[spec.table] = {Keys: []};
				}
				items[spec.table].Keys.push(DynamoDB.serializeKey(spec.key));
				return items;
			}, {});

			var params = {
				RequestItems: RequestItems
			};

			return dynamodb.batchGetItem(params).then(function (res) {
				Object.keys(res.Responses).forEach(function (tableName) {
					if (!results[tableName]) {
						results[tableName] = [];
					}
					results[tableName] = results[tableName].concat(res.Responses[tableName]);
				});

				if (rest.length) {
					return fetchPage(rest);
				}

				return results;
			});
		}

		return fetchPage(this.query(query));
	}
});

// spec.dynamodb
BatchGet.create = function (spec) {
	var obj = new BatchGet();
	return obj.initialize(spec);
};
