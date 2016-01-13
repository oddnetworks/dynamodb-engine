'use strict';
var util = require('util');
var constants = require('./constants');

var NOT_FOUND = constants.get('NOT_FOUND_ERROR');
var CONDITION_FAILED = constants.get('CONDITION_FAILED_ERROR');
var THROUGHPUT_EXCEEDED = constants.get('THROUGHPUT_EXCEEDED_ERROR');
var NONEXISTENT_TABLE = constants.get('NONEXISTENT_TABLE_ERROR');

function OperationalError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.message = message;
}
util.inherits(OperationalError, Error);
exports.OperationalError = OperationalError;

function DynamoDBError(spec) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = spec.code;
	this.message = spec.message;
}
util.inherits(DynamoDBError, OperationalError);
exports.DynamoDBError = DynamoDBError;

function NotFoundError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = NOT_FOUND;
	this.message = message;
}
util.inherits(NotFoundError, OperationalError);
exports.NotFoundError = NotFoundError;

function ConditionFailedError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = CONDITION_FAILED;
	this.message = message;
}
util.inherits(ConditionFailedError, OperationalError);
exports.ConditionFailedError = ConditionFailedError;

function ThroughputExceededError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = THROUGHPUT_EXCEEDED;
	this.message = message;
}
util.inherits(ThroughputExceededError, OperationalError);
exports.ThroughputExceededError = ThroughputExceededError;

function NonExistentTableError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = NONEXISTENT_TABLE;
	this.message = message;
}
util.inherits(NonExistentTableError, OperationalError);
exports.NonExistentTableError = NonExistentTableError;
