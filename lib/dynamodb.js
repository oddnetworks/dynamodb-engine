'use strict';
var U = require('./utils');

exports.attributeDefinitions = function attributeDefinitions(attrs) {
	attrs = attrs || Object.create(null);

	return Object.keys(attrs).map(function (key) {
		var type = attrs[key];
		if (!U.isString(type)) {
			throw new Error('DynamoDB Engine expects key value (type) to ' +
				'be a String.');
		}
		type = type.slice(0, 1).toUpperCase();
		if (['S', 'N', 'B'].indexOf(type) === -1) {
			throw new Error('DynamoDB Engine expects key value (type) to ' +
				'be "String", "Number", or "Boolean');
		}
		return {
			AttributeName: key,
			AttributeType: type
		};
	});
};

exports.keySchema = function (keys, attributes) {
	keys = keys || Object.create(null);
	var keySchema = [];

	if (!keys.hash && !keys.range) {
		throw new Error('DynamoDB Engine expects keys.hash and/or keys.range');
	}

	if (keys.hash) {
		if (!U.isString(keys.hash)) {
			throw new Error('DynamoDB Engine expects keys.hash to be a String.');
		}
		if (!U.find(attributes, 'AttributeName', keys.hash)) {
			throw new Error('DynamoDB requires attribute names in hash key also ' +
				'be defined in attributes');
		}
		keySchema.push({
			AttributeName: keys.hash,
			KeyType: 'HASH'
		});
	}

	if (keys.range) {
		if (!U.isString(keys.range)) {
			throw new Error('DynamoDB Engine expects keys.range to be a String.');
		}
		if (!U.find(attributes, 'AttributeName', keys.range)) {
			throw new Error('DynamoDB requires attribute names in range key also ' +
				'be defined in attributes');
		}
		keySchema.push({
			AttributeName: keys.range,
			KeyType: 'RANGE'
		});
	}

	return keySchema;
};

// record.id
// record.data
// options.idAttribute
exports.serializeRecord = function serializeRecord(record, options) {
	var idAttribute = options.idAttribute;
	var dynamoRecord = {};
	dynamoRecord[idAttribute] = {
		S: record.id
	};

	return Object.keys(record.data).reduce(function (rec, key) {
		var val;
		if (key !== idAttribute) {
			val = exports.typeCast(record.data[key]);
			if (val) {
				rec[key] = val;
			}
		}
		return rec;
	}, dynamoRecord);
};

exports.typeCast = function typeCast(obj) {
	switch (typeof obj) {
		case 'string':
			if (obj.length === 0) {
				throw new TypeError('Cannot set empty string attributes on DynamoDB.');
			}
			return {S: obj};
		case 'number':
			if (isNaN(obj)) {
				throw new TypeError('Cannot set NaN as an attribute on DynamoDB.');
			}
			return {N: obj.toString()};
		case 'boolean':
			return {BOOL: obj};
		case 'function':
			throw new TypeError('Cannot set a function as an attribute on DynamoDB.');
		case 'undefined':
			break;
		default:
			if (!obj) {
				return {NULL: true};
			}
			return Array.isArray(obj) ?
				exports.typeCastArray(obj) :
				exports.typeCastObject(obj);
	}
};

exports.typeCastArray = function typeCastArray(obj) {
	if (!obj.length) {
		return {NULL: true};
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

exports.typeCastObject = function typeCastObject(obj) {
	var keys = Object.keys(obj);
	var rv = {M: {}};
	if (!keys.length) {
		return rv;
	}

	rv.M = keys.reduce(function (M, key) {
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

exports.deserializeData = function deserializeData(obj) {
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
		} else if (val.hasOwnProperty('NULL')) {
			rv[key] = null;
		}
		return rv;
	}, Object.create(null));
};

exports.isResourceNotFoundException = function (err) {
	return err && err.code === 'ResourceNotFoundException';
};

exports.isProvisionedThroughputExceededException = function (err) {
	return err && err.code === 'ProvisionedThroughputExceededException';
};

exports.isTableExistsException = function (err) {
	return (err &&
		err.code === 'ResourceInUseException' &&
		/already\sexists/.test(err.message));
};

exports.isNonExistentTableException = function (err) {
	return (err &&
		err.code === 'ResourceNotFoundException' &&
		/non-existent\stable/.test(err.message));
};
