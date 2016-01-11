'use strict';
var uuid = require('node-uuid');

exports.extend = function (target, source) {
	return Object.keys(source).reduce(function (target, key) {
		target[key] = source[key];
		return target;
	}, target);
};

exports.isString = function (obj) {
	return typeof obj === 'string';
};

exports.find = function (arr, prop, val) {
	var i = arr.length - 1;
	for (i; i >= 0; i--) {
		if (arr[i] && arr[i][prop] === val) {
			return arr[i];
		}
	}
};

exports.uuid = function () {
	// A version 4 UUID is a random number generated UUID, rather than being
	// tied to the MAC address and a timestamp like version 1 UUIDs.
	return uuid.v4({rng: uuid.nodeRNG});
};
