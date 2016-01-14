'use strict';
var EventEmitter = require('events');
var util = require('util');
var Promise = require('bluebird');
var AWS = require('aws-sdk');
var U = require('./utils');
var constants = require('./constants');
var errors = require('./errors');
var DynamoDBError = errors.DynamoDBError;
var DynamoDB = require('./dynamodb');
var Table = require('./table');

var TABLE_EXISTS_ERROR = constants.get('TABLE_EXISTS_ERROR');
var NONEXISTENT_TABLE_ERROR = constants.get('NONEXISTENT_TABLE_ERROR');

var STREAM_TYPES = constants.get('STREAM_TYPES');

// Example spec:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   tableName: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
function Database(spec) {
	EventEmitter.init.call(this);
	var awsOptions = {
		accessKeyId: spec.accessKeyId,
		secretAccessKey: spec.secretAccessKey,
		region: spec.region,
		apiVersion: '2012-08-10'
	};
	if (spec.endpoint) {
		awsOptions.endpoint = spec.endpoint;
	}

	Object.defineProperties(this, {
		dynamodb: {
			enumerable: true,
			value: new AWS.DynamoDB(awsOptions)
		},
		tables: {
			value: Object.create(null)
		}
	});
}

util.inherits(Database, EventEmitter);

module.exports = Database;

U.extend(Database.prototype, {

	// Generic method of creating a DynamoDB table
	// AWS Docs: http://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/DynamoDB.html#createTable-property
	//
	// Example params:
	//
	// var params = {
	//   tableName: 'STRING_VALUE', /* required */
	//   attributes: { /* required */
	//     someAttributeName: 'String | Number',
	//     /* all attributes used in keys or globalIndex keys */
	//   },
	//   keys: { /* required */
	//     hash: 'String | Number', /* required if there is not a unique range key */
	//     range: 'String | Number'
	//   },
	//   throughput: {
	//     read: 10, /* default = 10 */
	//     write: 5 /* default = 5 */
	//   },
	//   globalIndexes: [
	//     {
	//       indexName: 'STRING_VALUE', /* required */
	//       keys: { /* required */
	//         hash: 'String | Number', /* required if there is not a unique range key */
	//         range: 'String | Number'
	//       },
	//       throughput: { /* required */
	//         read: 10, /* default = 10 */
	//         write: 5 /* default = 5 */
	//       }
	//     }
	//   ],
	//   stream: 'NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY'
	// };
	//
	createTable: function (params) {
		params = params || Object.create(null);
		var self = this;

		var tableName = params.tableName || params.TableName;
		if (!tableName || !U.isString(tableName)) {
			throw new Error('Database#createTable(params) expects ' +
				'params.tableName to be a non-empty String');
		}

		var attributes = params.attributes || params.AttributeDefinitions;
		if (!attributes) {
			throw new Error('Database#createTable(params) expects ' +
				'params.attributes to be an Object');
		}

		var AttributeDefinitions = DynamoDB.attributeDefinitions(attributes);
		var throughput = params.throughput || params.ProvisionedThroughput || {};
		var stream = params.stream || params.StreamSpecification;
		var globalIndexes = params.globalIndexes || params.GlobalSecondaryIndexes;

		var newParams = {
			TableName: tableName,
			AttributeDefinitions: AttributeDefinitions,
			KeySchema: DynamoDB.keySchema(params.keys, AttributeDefinitions),
			ProvisionedThroughput: {
				ReadCapacityUnits: throughput.read ||
					throughput.ReadCapacityUnits || 10,
				WriteCapacityUnits: throughput.write ||
					throughput.WriteCapacityUnits || 5
			}
		};

		if (globalIndexes && globalIndexes.length) {
			newParams.GlobalSecondaryIndexes = globalIndexes.map(function (spec) {
				var throughput = spec.throughput || spec.ProvisionedThroughput || {};
				return {
					IndexName: spec.indexName || spec.IndexName,
					KeySchema: DynamoDB.keySchema(spec.keys, AttributeDefinitions),
					Projection: {ProjectionType: 'ALL'},
					ProvisionedThroughput: {
						ReadCapacityUnits: throughput.read ||
							throughput.ReadCapacityUnits || 10,
						WriteCapacityUnits: throughput.write ||
							throughput.WriteCapacityUnits || 5
					}
				};
			});
		}

		if (stream) {
			if (STREAM_TYPES.indexOf(stream) === -1) {
				throw new Error('dynamoDBEngine.createTable(params) expects ' +
					'params.stream to be "' + STREAM_TYPES.join(' | ') + '"');
			}
			newParams.StreamSpecification = {
				StreamEnabled: true,
				StreamViewType: stream
			};
		}

		return new Promise(function (resolve, reject) {
			self.dynamodb.waitFor(
				'tableExists',
				{TableName: newParams.TableName},
				function (err, res) {
					if (err) {
						return reject(new DynamoDBError(err));
					}
					resolve(res.Table);
				});

			self.dynamodb.createTable(newParams, function (err) {
				if (DynamoDB.isTableExistsException(err)) {
					self.emit('warning', {
						code: TABLE_EXISTS_ERROR,
						message: err.message
					});
				} else if (err) {
					reject(new DynamoDBError(err));
				}
			});
		});
	},

	// Example params:
	//
	// var params = {
	//   tableName: 'STRING_VALUE' /* required */
	// };
	deleteTable: function (params) {
		params = params || {};

		var newParams = {
			TableName: params.tableName || params.TableName
		};

		if (!newParams.TableName || !U.isString(newParams.TableName)) {
			throw new Error('Database#deleteTable(params) expects ' +
				'params.tableName to be a non-empty String');
		}

		var self = this;

		return new Promise(function (resolve, reject) {
			var rv = null;

			self.dynamodb.waitFor(
				'tableNotExists',
				{TableName: newParams.TableName},
				function (err) {
					if (err) {
						return reject(err);
					}
					if (rv) {
						return resolve(rv);
					}
					rv = true;
				});
			self.dynamodb.deleteTable(newParams, function (err, res) {
				if (DynamoDB.isResourceNotFoundException(err)) {
					self.emit('warning', {
						code: NONEXISTENT_TABLE_ERROR,
						message: err.message
					});
				} else if (err) {
					reject(new DynamoDBError(err));
				} else if (rv) {
					return resolve(res.TableDescription);
				}
				rv = res.TableDescription;
			});
		});
	},

	// Returns a Promise for a list of tables (Array of Strings) available to
	// the credentials in this DynamoDBEngine instance.
	listTables: function () {
		var self = this;
		return new Promise(function (resolve, reject) {
			self.dynamodb.listTables({}, function (err, res) {
				if (err) {
					return reject(err);
				}
				resolve(res.TableNames);
			});
		});
	},

	useTable: function (tableName) {
		if (!tableName) {
			throw new Error('dynamoDBEngine.useTable(tableName) requires a ' +
				'tableName parameter');
		}
		if (!U.isString(tableName)) {
			throw new Error('dynamoDBEngine.useTable(tableName) tableName ' +
				'must be a String');
		}

		// Return the cached table if we have it.
		var table = this.tables[tableName];
		if (table) {
			return table;
		}

		// Else create a new table object and cache it.
		table = Table.create({
			tableName: tableName,
			dynamodb: this.dynamodb
		});
		table.on('error', function (err) {
			this.emit('error', err);
		});
		table.on('warning', function (warning) {
			this.emit('warning', warning);
		});
		Object.defineProperty(this.tables, tableName, {
			value: table
		});
		return table;
	}
});

// Example spec:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   tableName: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
Database.create = function (spec) {
	return new Database(spec);
};
