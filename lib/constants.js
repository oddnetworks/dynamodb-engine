'use strict';

exports.LOGLEVELS_WARN = 'WARN';
exports.LOGLEVELS_INFO = 'INFO';
exports.LOGLEVELS_DEBUG = 'DEBUG';

exports.STREAM_TYPES = Object.freeze([
	'KEYS_ONLY',
	'OLD_IMAGE',
	'NEW_AND_OLD_IMAGES',
	'NEW_IMAGE'
]);

exports.NOT_FOUND_ERROR = 'DYNAMODB_NOT_FOUND';
exports.THROUGHPUT_EXCEEDED_ERROR = 'DYNAMODB_THROUGHPUT_EXCEEDED';
exports.NONEXISTENT_TABLE_ERROR = 'DYNAMODB_NONEXISTENT_TABLE';
exports.TABLE_EXISTS_ERROR = 'DYNAMODB_TABLE_EXISTS';

exports.get = function (key) {
	if (Object.prototype.hasOwnProperty.call(exports, key)) {
		return exports[key];
	}
	throw new Error('Constant ' + key + ' does not exist.');
};
