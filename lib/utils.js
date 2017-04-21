var U = require('lodash');
var brixx = require('brixx');

function isFullString(str) {
	return str && typeof str === 'string';
}

function isActualNumber(n) {
	return typeof n === 'number' && !isNaN(n);
}

function returnUndefined() {
}

U.mixin({
	ensure: brixx.ensure,
	deepFreeze: brixx.deepFreeze,
	exists: brixx.exists,
	stringify: brixx.stringify,
	returnUndefined: returnUndefined,
	isFullString: isFullString,
	isActualNumber: isActualNumber
});

module.exports = U;
