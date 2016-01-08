'use strict';
var util = require('util');

exports.NOT_FOUND = 'DYNAMODB_NOT_FOUND';
exports.THROUGHPUT_EXCEEDED = 'DYNAMODB_THROUGHPUT_EXCEEDED';

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
	this.code = exports.NOT_FOUND;
	this.message = message;
}
util.inherits(NotFoundError, OperationalError);
exports.NotFoundError = NotFoundError;

function ThroughputExceededError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = exports.THROUGHPUT_EXCEEDED;
	this.message = message;
}
util.inherits(ThroughputExceededError, OperationalError);
exports.ThroughputExceededError = ThroughputExceededError;
