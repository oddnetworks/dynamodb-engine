'use strict';
var util = require('utils');
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

// record.id
// record.data
exports.serializeRecord = function (record, idAttribute) {
	var dynamoRecord = {};
	dynamoRecord[idAttribute] = {
		S: record.id
	};

	Object.keys(record.data).reduce(function (rec, key) {
		var val;
		if (key !== idAttribute) {
			rec[key] = exports.typeCast(record.data[key]);
			if (val) {
				rec[key] = val;
			}
		}
		return rec;
	}, dynamoRecord);
};

exports.typeCast = function (obj) {
	switch (typeof obj) {
		case 'string':
			return {S: obj};
		case 'number':
			if (isNaN(obj)) {
				throw new TypeError('Cannot set NaN as a number on DynamoDB.');
			}
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		case 'function':
			throw new TypeError('Cannot set a function value on DynamoDB.');
		case 'undefined':
			break;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ? exports.typeCastArray(obj) : exports.typeCastObject(obj);
	}
};

exports.typeCastArray = function (obj) {
	if (!obj.length) {
		return [];
	}

	var rv = {};
	var type = typeof obj[0];
	var key;

	// We only accept String Lists or Number Lists.
	if (type === 'string') {
		key = 'SS';
	} else if (type === 'number') {
		key = 'NS';
	} else {
		throw new TypeError('Only String or Number Lists (not ' + type +
												') may be defined in DynamoDB Engine.');
	}

	rv[key] = obj.map(function (val) {
		if (typeof val !== type) {
			throw new TypeError('All values in a String or Number List must be of ' +
													'the same type in DynamoDB Engine.');
		}
		return val;
	});

	return rv;
};

exports.typeCastObject = function (obj) {
	var keys = Object.keys(obj);
	var rv = {M: {}};
	if (!keys.length) {
		return rv;
	}

	rv.M = Object.keys(obj).reduce(function (M, key) {
		var type = typeof obj[key];
		var val = obj[key];
		switch (type) {
			case 'string':
				M[key] = {S: val};
				break;
			case 'number':
				M[key] = {N: val};
				break;
			case 'boolean':
				M[key] = {BOOL: val};
				break;
			default:
				throw new TypeError('Only String, Number or Boolean attributes ' +
														'(not ' + type + ') may be defined on Mixed ' +
														'Objects in DynamoDB Engine.');
		}
		return M;
	}, rv.M);

	return rv;
};

exports.deserializeData = function (obj) {
	return Object.keys(obj).reduce(function (rv, key) {
		var val = obj[key];
		if (val.hasOwnProperty('S')) {
			rv[key] = val.S.toString();
		} else if (val.hasOwnProperty('N')) {
			rv[key] = val.N;
		} else if (val.SS || val.NS) {
			rv[key] = val.SS || val.NS;
		} else if (val.hasOwnProperty('BOOL')) {
			rv[key] = Boolean(val.BOOL);
		} else if (val.hasOwnProperty('M')) {
			rv[key] = exports.deserializeData(val.M);
		}
		return rv;
	}, Object.create(null));
};

exports.uuid = function () {
};

function OperationalError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.message = message;
}
util.inherits(OperationalError, Error);
exports.OperationalError = OperationalError;

function NotFoundError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.message = message;
}
util.inherits(NotFoundError, OperationalError);
exports.NotFoundError = NotFoundError;
