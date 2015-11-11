'use strict';
var Promise = require('bluebird');
var AWS = require('aws-sdk');

// spec.accessKeyId - *String*
// spec.secretAccessKey = *String*
// spec.region - *String*
// spec.sslEnabled - *Boolean*
// spec.apiVersion - *String* default: 'latest'
// spec.logger - *Object* that responds to #write() or #log()
exports.createEngine = function (spec) {
	var self = Object.create(null);
	var awsOptions = {
		accessKeyId: spec.accessKeyId,
		secretAccessKey: spec.secretAccesskey,
		region: spec.region,
		sslEnabled: Boolean(spec.sslEnabled),
		apiVersion: spec.apiVersion || 'latest',
		logger: spec.logger
	};
	self.dynamodb = new AWS.DynamoDB(awsOptions);

	self.get = function (id) {
		if (arguments.length < 1) {
			throw new Error('dynamoDBEngine.get(id) requires an ID String as the ' +
											'first argument.');
		}
		if (typeof id !== 'string') {
			throw new Error('dynamoDBEngine.get(id) requires an ID String as the ' +
											'first argument.');
		}

		return new Promise(function (resolve, reject) {
		});
	};

	self.post = function (record) {
		if (record.id) {
			throw new Error('dynamoDBEngine.post(record) must not have a ' +
											'record id.');
		}

		return new Promise(function (resolve, reject) {
		});
	};

	self.put = function (record) {
		if (typeof record.id !== 'string' || !record.id) {
			throw new Error('dynamoDBEngine.put(record) must have a String ' +
											'record id.');
		}

		return new Promise(function (resolve, reject) {
		});
	};

	self.remove = function (id) {
		if (arguments.length < 1) {
			throw new Error('dynamoDBEngine.remove(id) requires an ID String as the ' +
											'first argument.');
		}
		if (typeof id !== 'string') {
			throw new Error('dynamoDBEngine.remove(id) requires an ID String as the ' +
											'first argument.');
		}

		return new Promise(function (resolve, reject) {
		});
	};

	return self;
};
