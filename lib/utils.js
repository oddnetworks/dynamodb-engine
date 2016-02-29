var U = require('lodash');
var brixx = require('brixx');

U.mixin({
	ensure: brixx.ensure,
	deepFreeze: brixx.deepFreeze,
	exists: brixx.exists,
	stringify: brixx.stringify,
	returnUndefined: function returnUndefined() {
		return;
	}
});

module.exports = U;
