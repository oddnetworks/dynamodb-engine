'use strict';
var U = require('./utils');
var DynamoDB = require('./dynamodb');
var debug = require('debug')('dynamodb-engine:batch_get');

function BatchGet() {}

module.exports = BatchGet;

U.extend(BatchGet.prototype, {
	initialize: function (spec) {
		Object.defineProperties(this, {
			dynamodb: {
				value: spec.dynamodb
			},
			tables: {
				value: Object.create(null)
			}
		});

		return this;
	},

	// query - Object
	//   query[table] - Array
	//     query[table][i] - Object
	//       query[table][i][key] - String or Number
	query: function (query) {
		var self = this;

		var queries = Object.keys(query).reduce(function (items, type) {
			var keys = query[type];
			var table = self.dynamodb.table(type);
			self.tables[table] = type;
			return items.concat(keys.map(function (key) {
				return {
					table: table,
					key: key
				};
			}));
		}, []);

		Object.freeze(this.tables);
		return queries;
	},

	// DynamoDB only allows us to get items 100 at a time, so we have to
	// jump through some hoops to follow the rulz.
	//
	// query - Object
	//   query[type] - Array
	//     query[type][i] - Object
	//       query[type][i][key] - String or Number
	fetchAll: function (query) {
		var self = this;

		function fetchPage(items, results) {
			debug('fetchAll() fetchPage()');
			results = results || Object.create(null);
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

			function handleBatchGetResponse(res) {
				Object.keys(res.Responses).forEach(function (tableName) {
					if (!results[tableName]) {
						results[tableName] = [];
					}
					var responses = res.Responses[tableName].map(DynamoDB.deserializeItem);
					results[tableName] = results[tableName].concat(responses);
				});
				return results;
			}

			function next(results) {
				if (rest.length) {
					debug('fetchAll() fetchPage() %d items remain', rest.length);
					return fetchPage(rest, results);
				}
				debug('fetchAll() fetchPage() all pages complete');
				return results;
			}

			return self.dynamodb
				.batchGetItem(params)
				.then(handleBatchGetResponse)
				.then(next);
		}

		var queries = this.query(query);
		debug('fetchAll() %d', queries.length);

		return fetchPage(queries).then(function mapResults(res) {
			var count = 0;

			var results = Object.keys(res).reduce(function (items, table) {
				var type = self.tables[table];
				count += res[table].length;
				items[type] = res[table];
				return items;
			}, Object.create(null));

			debug('fetchAll() got %d results', count);
			return results;
		});
	}
});

// spec.dynamodb
BatchGet.create = function (spec) {
	var obj = new BatchGet();
	return obj.initialize(spec);
};
