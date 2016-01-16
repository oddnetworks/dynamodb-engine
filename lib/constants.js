'use strict';

var constants = Object.create(null);

exports.get = function (key) {
	if (Object.prototype.hasOwnProperty.call(constants, key)) {
		return constants[key];
	}
	throw new Error('Constant ' + key + ' does not exist.');
};

constants.LOGLEVELS_WARN = 'WARN';
constants.LOGLEVELS_INFO = 'INFO';
constants.LOGLEVELS_DEBUG = 'DEBUG';

constants.STREAM_TYPES = Object.freeze([
	'KEYS_ONLY',
	'OLD_IMAGE',
	'NEW_AND_OLD_IMAGES',
	'NEW_IMAGE'
]);

constants.NOT_FOUND_ERROR = 'DYNAMODB_NOT_FOUND';
constants.CONDITION_FAILED_ERROR = 'DYNAMODB_CONDITION_FAILED';
constants.THROUGHPUT_EXCEEDED_ERROR = 'DYNAMODB_THROUGHPUT_EXCEEDED';
constants.NONEXISTENT_TABLE_ERROR = 'DYNAMODB_NONEXISTENT_TABLE';
constants.TABLE_EXISTS_ERROR = 'DYNAMODB_TABLE_EXISTS';

Object.freeze(constants);
