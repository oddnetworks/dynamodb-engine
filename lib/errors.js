'use strict';
var util = require('util');
var constants = require('./constants');

var NOT_FOUND_ERROR = constants.get('NOT_FOUND_ERROR');
var CONDITION_FAILED_ERROR = constants.get('CONDITION_FAILED_ERROR');
var THROUGHPUT_EXCEEDED_ERROR = constants.get('THROUGHPUT_EXCEEDED_ERROR');
var NONEXISTENT_TABLE_ERROR = constants.get('NONEXISTENT_TABLE_ERROR');
var NONEXISTENT_INDEX_ERROR = constants.get('NONEXISTENT_INDEX_ERROR');
var TABLE_NOT_ACTIVE_ERROR = constants.get('TABLE_NOT_ACTIVE_ERROR');
var TABLE_EXISTS_ERROR = constants.get('TABLE_EXISTS_ERROR');
var CONFLICT_ERROR = constants.get('CONFLICT_ERROR');
var SCHEMA_ERROR = constants.get('SCHEMA_ERROR');

var errors = Object.create(null);

exports.get = function (key) {
	if (Object.prototype.hasOwnProperty.call(errors, key)) {
		return errors[key];
	}
	throw new Error('Error type ' + key + ' does not exist.');
};

function OperationalError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.message = message;
}
util.inherits(OperationalError, Error);
errors.OperationalError = OperationalError;

function DynamoDBError(spec) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = spec.code;
	this.message = spec.message;
}
util.inherits(DynamoDBError, OperationalError);
errors.DynamoDBError = DynamoDBError;

function NotFoundError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = NOT_FOUND_ERROR;
	this.message = message;
}
util.inherits(NotFoundError, OperationalError);
errors.NotFoundError = NotFoundError;

function ConditionFailedError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = CONDITION_FAILED_ERROR;
	this.message = message;
}
util.inherits(ConditionFailedError, OperationalError);
errors.ConditionFailedError = ConditionFailedError;

function ThroughputExceededError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = THROUGHPUT_EXCEEDED_ERROR;
	this.message = message;
}
util.inherits(ThroughputExceededError, OperationalError);
errors.ThroughputExceededError = ThroughputExceededError;

function NonExistentTableError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = NONEXISTENT_TABLE_ERROR;
	this.message = message;
}
util.inherits(NonExistentTableError, OperationalError);
errors.NonExistentTableError = NonExistentTableError;

function NonExistentIndexError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = NONEXISTENT_INDEX_ERROR;
	this.message = message;
}
util.inherits(NonExistentIndexError, OperationalError);
errors.NonExistentIndexError = NonExistentIndexError;

function TableExistsError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = TABLE_EXISTS_ERROR;
	this.message = message;
}
util.inherits(TableExistsError, OperationalError);
errors.TableExistsError = TableExistsError;

function TableNotActiveError(tableName) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = TABLE_NOT_ACTIVE_ERROR;
	this.message = 'The table or index ' + tableName + ' is not ACTIVE';
}
util.inherits(TableNotActiveError, OperationalError);
errors.TableNotActiveError = TableNotActiveError;

function ConflictError(id) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = CONFLICT_ERROR;
	this.message = 'Record id ' + id + ' already exists';
}
util.inherits(ConflictError, OperationalError);
errors.ConflictError = ConflictError;

function SchemaError(message) {
	Error.call(this);
	Error.captureStackTrace(this, this.constructor);
	this.name = this.constructor.name;
	this.code = SCHEMA_ERROR;
	this.message = message;
}
util.inherits(SchemaError, Error);
errors.SchemaError = SchemaError;

Object.freeze(errors);
