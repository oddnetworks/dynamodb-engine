'use strict';

var API = Object.create(null);

// options:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
API.initialize = function () {
	if (this.initialized) {
		return this;
	}

	Object.defineProperies(this, {
		initialized: {
			value: true
		}
	});

	return this;
};

API.migrateUp = function migrateUp() {
};

API.migrateDown = function migrateDown() {
};

API.addReleationship = function () {
};

exports.API = API;

// options:
// {
//   accessKeyId: 'STRING_VALUE', /* required */
//   secretAccessKey: 'STRING_VALUE', /* required */
//   region: 'STRING_VALUE', /* required */
//   endpoint: 'STRING_VALUE', /* The AWS URL for DynamoDB is the default */
// }
exports.create = function (options) {
	var api = Object.create(API);
	return api.initialize(options);
};
