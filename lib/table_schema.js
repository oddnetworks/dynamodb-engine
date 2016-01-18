'use strict';
var U = require('./utils');
var errors = require('./errors');
var SchemaError = errors.get('SchemaError');

function TableSchema(spec) {
	Object.defineProperties(this, {
		TableName: {
			enumerable: true,
			value: spec.TableName
		},
		AttributeDefinitions: {
			enumerable: true,
			value: Object.freeze(spec.AttributeDefinitions)
		},
		KeySchema: {
			enumerable: true,
			value: Object.freeze(spec.KeySchema)
		},
		ProvisionedThroughput: {
			enumerable: true,
			value: Object.freeze(spec.ProvisionedThroughput)
		},
		GlobalSecondaryIndexes: {
			enumerable: true,
			value: spec.GlobalSecondaryIndexes ?
				Object.freeze(spec.GlobalSecondaryIndexes) : null
		},
		StreamSpecification: {
			enumerable: true,
			value: spec.StreamSpecification ?
				Object.freeze(spec.StreamSpecification) : null
		},
		TableStatus: {
			enumerable: true,
			value: spec.TableStatus || null
		},
		CreationDateTime: {
			enumerable: true,
			value: spec.CreationDateTime || null
		},
		TableSizeBytes: {
			enumerable: true,
			value: U.isNumber(spec.TableSizeBytes) ? spec.TableSizeBytes : null
		},
		ItemCount: {
			enumerable: true,
			value: U.isNumber(spec.ItemCount) ? spec.ItemCount : null
		},
		TableArn: {
			enumerable: true,
			value: spec.TableArn || null
		},
		LatestStreamLabel: {
			enumerable: true,
			value: spec.LatestStreamLabel || null
		},
		LatestStreamArn: {
			enumerable: true,
			value: spec.LatestStreamArn || null
		}
	});
}

module.exports = TableSchema;

U.extend(TableSchema.prototype, {
	createTableParams: function () {
		var params = {
			TableName: this.TableName,
			AttributeDefinitions: this.AttributeDefinitions.map(function (attr) {
				return attr.createTableParams();
			}),
			KeySchema: this.KeySchema.map(function (key) {
				return key.createTableParams();
			}),
			ProvisionedThroughput: this.ProvisionedThroughput.createTableParams()
		};

		if (this.GlobalSecondaryIndexes) {
			params.GlobalSecondaryIndexes = this.GlobalSecondaryIndexes
				.map(function (gsi) {
					return gsi.createTableParams();
				});
		}
		if (this.StreamSpecification) {
			params.StreamSpecification = this.StreamSpecification
				.createTableParams();
		}

		return params;
	},

	updateDelta: function (current) {
		var self = this;
		var update = false;
		var pendingIndexes = [];

		var indexes = this.GlobalSecondaryIndexes.reduce(function (indexes, spec) {
			var currentIndex = U.find(current.GlobalSecondaryIndexes, {
				IndexName: spec.IndexName
			});
			if (!currentIndex) {
				indexes.push({Create: spec});
				pendingIndexes.push(spec);
			}
			if (!U.isEqual(currentIndex.KeySchema, spec.KeySchema)) {
				throw new SchemaError('Cannot change KeySchema of a ' +
					'GlobalSecondaryIndex: ' + self.TableName + ':' + spec.IndexName);
			}
			return indexes;
		}, []);

		if (indexes.length) {
			this.KeySchema.forEach(function (spec) {
				var def = U.find(self.AttributeDefinitions, {
					AttributeName: spec.AttributeName
				});
				if (!def) {
					throw new SchemaError('Attributes found in KeySchema not in ' +
						'AttributeDefinitions: ' + self.TableName);
				}
			});

			pendingIndexes.forEach(function (index) {
				index.KeySchema.forEach(function (spec) {
					var def = U.find(self.AttributeDefinitions, {
						AttributeName: spec.AttributeName
					});
					if (!def) {
						throw new SchemaError('Attributes found in KeySchema not in ' +
							'AttributeDefinitions: ' + self.TableName + ':' + index.IndexName);
					}
				});
			});

			update = {
				TableName: current.TableName,
				AttributeDefinitions: this.AttributeDefinitions,
				ProvisionedThroughput: current.ProvisionedThroughput,
				StreamSpecification: current.StreamSpecification,
				GlobalSecondaryIndexUpdates: indexes
			};
		}

		return update;
	}
});

TableSchema.create = function (spec) {
	var attrs = spec.AttributeDefinitions.map(AttributeDefinition.create);
	var keys = spec.KeySchema.map(KeySchema.create);
	var throughput = ProvisionedThroughput.create(spec.ProvisionedThroughput);
	var gsi = spec.GlobalSecondaryIndexes.map(GlobalSecondaryIndex.create);
	var streams = StreamSpecification.create(spec.StreamSpecification);

	return new TableSchema({
		TableName: spec.TableName,
		AttributeDefinitions: attrs,
		KeySchema: keys,
		ProvisionedThroughput: throughput,
		GlobalSecondaryIndexes: gsi,
		StreamSpecification: streams,
		TableStatus: spec.TableStatus,
		CreationDateTime: spec.CreationDateTime,
		TableSizeBytes: spec.TableSizeBytes,
		ItemCount: spec.ItemCount,
		TableArn: spec.TableArn,
		LatestStreamLabel: spec.LatestStreamLabel,
		LatestStreamArn: spec.LatestStreamArn
	});
};

// def.tableName - String *required
// def.attributes - Object *required
//   [someAttributeName] - String *required 'String | Number | Boolean'
// def.keys - Object *required
//   .hash - String *required
//   .range - String
// def.throughput - Object *required
//   .read - Number *required
//   .write - Number *required
// def.streams - String 'NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY'
// def.indexes - Array of Objects
//   [i] - Object
//     .indexName - String *required
//     .keys - Object *required
//       .hash - String *required
//       .range - String
//     .projection - String *required 'ALL | KEYS_ONLY | INCLUDE'
//     .throughput - Object *required
//       .read - Number *required
//       .write - Number *required
TableSchema.createFromDefinition = function (def) {
	var attrs = AttributeDefinition.createFromDefinitions(def.attributes);
	var keys = KeySchema.createFromDefinition(def.keys);
	var throughput = ProvisionedThroughput.createFromDefinition(def.throughput);
	var gsi = GlobalSecondaryIndex.createFromDefinitions(def.indexes);
	var streams = StreamSpecification.createFromDefinition(def.streams);

	return new TableSchema({
		TableName: def.tableName,
		AttributeDefinitions: attrs,
		KeySchema: keys,
		ProvisionedThroughput: throughput,
		GlobalSecondaryIndexes: gsi,
		StreamSpecification: streams
	});
};

function AttributeDefinition(spec) {
	Object.defineProperties(this, {
		AttributeName: {
			enumerable: true,
			value: spec.AttributeName
		},
		AttributeType: {
			enumerable: true,
			value: spec.AttributeType
		}
	});
}
U.extend(AttributeDefinition.prototype, {
	createTableParams: function () {
		var self = this;
		return Object.keys(self).reduce(function (params, k) {
			params[k] = self[k];
			return params;
		}, {});
	}
});
AttributeDefinition.create = function (spec) {
	return new AttributeDefinition(spec);
};
AttributeDefinition.createFromDefinitions = function (definitions) {
	return Object.keys(definitions).map(function (key) {
		return new AttributeDefinition({
			AttributeName: key,
			AttributeType: definitions[key].slice(0, 1).toUpperCase()
		});
	});
};

function KeySchema(spec) {
	Object.defineProperties(this, {
		AttributeName: {
			enumerable: true,
			value: spec.AttributeName
		},
		KeyType: {
			enumerable: true,
			value: spec.KeyType
		}
	});
}
U.extend(KeySchema.prototype, {
	createTableParams: function () {
		var self = this;
		return Object.keys(self).reduce(function (params, k) {
			params[k] = self[k];
			return params;
		}, {});
	}
});
KeySchema.create = function (spec) {
	return new KeySchema(spec);
};
KeySchema.createFromDefinition = function (def) {
	var rv = [
		new KeySchema({AttributeName: def.hash, KeyType: 'HASH'})
	];
	if (def.range) {
		rv.push(new KeySchema({AttributeName: def.range, KeyType: 'RANGE'}));
	}
	return rv;
};

function ProvisionedThroughput(spec) {
	Object.defineProperties(this, {
		ReadCapacityUnits: {
			enumerable: true,
			value: spec.ReadCapacityUnits
		},
		WriteCapacityUnits: {
			enumerable: true,
			value: spec.WriteCapacityUnits
		}
	});
}
U.extend(ProvisionedThroughput.prototype, {
	createTableParams: function () {
		var self = this;
		return Object.keys(self).reduce(function (params, k) {
			params[k] = self[k];
			return params;
		}, {});
	}
});
ProvisionedThroughput.create = function (spec) {
	return new ProvisionedThroughput(spec);
};
ProvisionedThroughput.createFromDefinition = function (def) {
	return new ProvisionedThroughput({
		ReadCapacityUnits: def.read,
		WriteCapacityUnits: def.write
	});
};

function GlobalSecondaryIndex(spec) {
	Object.defineProperties(this, {
		IndexName: {
			enumerable: true,
			value: spec.IndexName
		},
		KeySchema: {
			enumerable: true,
			value: Object.freeze(spec.KeySchema)
		},
		Projection: {
			enumerable: true,
			value: Object.freeze(spec.Projection)
		},
		ProvisionedThroughput: {
			enumerable: true,
			value: Object.freeze(spec.ProvisionedThroughput)
		}
	});
}
U.extend(GlobalSecondaryIndex.prototype, {
	createTableParams: function () {
		return {
			IndexName: this.IndexName,
			KeySchema: this.KeySchema.map(function (key) {
				return key.createTableParams();
			}),
			Projection: this.Projection.createTableParams(),
			ProvisionedThroughput: this.ProvisionedThroughput.createTableParams()
		};
	}
});
GlobalSecondaryIndex.create = function (spec) {
	var keys = spec.KeySchema.map(KeySchema.create);
	var throughput = ProvisionedThroughput.create(spec.ProvisionedThroughput);
	return new GlobalSecondaryIndex({
		IndexName: spec.IndexName,
		KeySchema: keys,
		Projection: Projection.create(spec.Projection),
		ProvisionedThroughput: throughput
	});
};
GlobalSecondaryIndex.createFromDefinitions = function (definitions) {
	if (!definitions) {
		return null;
	}
	return definitions.map(function (def) {
		var keys = KeySchema.createFromDefinition(def.keys);
		var throughput = ProvisionedThroughput.createFromDefinition(def.throughput);
		return new GlobalSecondaryIndex({
			IndexName: def.indexName,
			KeySchema: keys,
			Projection: Projection.createFromDefinition(def.projection),
			ProvisionedThroughput: throughput
		});
	});
};

function StreamSpecification(spec) {
	Object.defineProperties(this, {
		StreamEnabled: {
			enumerable: true,
			value: spec.StreamEnabled
		},
		StreamViewType: {
			enumerable: true,
			value: spec.StreamViewType
		}
	});
}
U.extend(StreamSpecification.prototype, {
	createTableParams: function () {
		var self = this;
		return Object.keys(self).reduce(function (params, k) {
			params[k] = self[k];
			return params;
		}, {});
	}
});
StreamSpecification.create = function (spec) {
	return new StreamSpecification(spec);
};
StreamSpecification.createFromDefinition = function (def) {
	if (!def) {
		return null;
	}
	return new StreamSpecification({
		StreamEnabled: true,
		StreamViewType: def
	});
};

function Projection(spec) {
	Object.defineProperties(this, {
		NonKeyAttributes: {
			enumerable: true,
			value: spec.NonKeyAttributes ?
				Object.freeze(spec.NonKeyAttributes) : null
		},
		ProjectionType: {
			enumerable: true,
			value: spec.ProjectionType
		}
	});
}
U.extend(Projection.prototype, {
	createTableParams: function () {
		var params = {
			ProjectionType: this.ProjectionType
		};
		if (this.NonKeyAttributes) {
			params.NonKeyAttributes = this.NonKeyAttributes;
		}
		return params;
	}
});
Projection.create = function (spec) {
	return new Projection(spec);
};
Projection.createFromDefinition = function (def) {
	return new Projection({
		ProjectionType: def
	});
};
