DynamoDB Engine
===============
A promisified Node.js engine for AWS DynamoDB.

API
---
### Create an Instance
```JS
const DynamoDBEngine = require('dynamodb-engine');

const config = {
  accessKeyId: process.env.DYNAMODB_ACCESS_KEY_ID,
  secretAccessKey: process.env.DYNAMODB_SECRET_ACCESS_KEY,
  region: process.env.DYNAMODB_REGION,
  endpoint: process.env.DYNAMODB_ENDPOINT,
  tablePrefix: 'myapp_prod'
};

// A schema for comic book characters and series.
const schema = {
  // A Character table with an index by name attribute
  Character: {
    indexes: {
      ByName: {
        keys: {
          hash: {name: 'type', type: 'String'},
          range: {name: 'name', type: 'String'}
        }
      }
    }
  },
  // A Character table with an index by title attribute
  Series: {
    indexes: {
      ByTitle: {
        keys: {
          hash: {name: 'type', type: 'String'},
          range: {name: 'title', type: 'String'}
        }
      }
    }
  }
};

const db = DynamoDBEngine.create(config, schema);
```

#### Schema
A table must be created for every document type you plan on storing. If you add another type to your application, add it to the schema and then run `.migrateUp()`.

### migrateUp
`.migrateUp()` -- Runs through the schema and ensures all the tables and indexes exist as defined. If you add any tables or indexes, you'll need to run `.migrateUp()`.

Returns a Promise for the response from the AWS SDK.

### createRecord
`.createRecord(record)` -- Stores the given record Object. All the document attributes should be present on the record. The `.type` and `.id` attributes are required.

Returns a Promise for the same record Object that was passed in.

If there is already an existing document with the given `record.id` then the returned Promise will reject with a `db.ConflictError`.

### updateRecord
`.updateRecord(record)` -- Stores the given record Object. All the document attributes should be present on the record. The `.type` and `.id` attributes are required.

If there is already an existing document with the given `record.id` then `.updateRecord(record)` will overwrite the existing document.

Returns a Promise for the same record Object that was passed in.

### getRecord
`.getRecord(type, id)` -- Retrieves the document identified by the type and id Strings.

Returns a Promise for the document Object.

If the document does not exist the returned Promise will reject with a `db.NotFoundError`.

### removeRecord
`.removeRecord(type, id)` -- Removes a document identified by the type and id Strings.

Returns a Promise for a Boolean `true`.

### createRelation
`.createRelation(subject, object)` -- Simultaniously adds a *subject has object* and *object belongs to* relationship to the relationships table and indexes. The relationships table and indexes are created when you first run `migrateUp()`.

Takes a subject and object Objects as parameters. Both the subject and the object must have a `.type` and `.id` attribute. Any other attributes will be ignored.

Returns a Promise for the object Object.

### getRelations
`.getRelations(subjectId, predicate)` -- Fetches a list of ID Strings for items of type `predicate` related to `subjectId`. Returns a list of *has many* relationships.

Example: `db.getRelations('series-abc-123', 'Charaacter')` would list all the Character IDs related to Series `series-abc-123`.

Returns a Promise for an Array of Objects like `{type, id}`.

### getReverseRelations
`.getReverseRelations(objectId, predicate)` -- Like `.getRelations()` but reversed. It returns a list of *belongs to* relationships.

Example: `db.getRelations('character-dfg-456', 'Series')` would list all the Series IDs which Character `character-dfg-456` belongs to.

Returns a Promise for an Array of Objects like `{type, id}`.

### removeRelation
`.removeRelation(subjectId, objectId)` -- Removes the record from the relations table and indexes linking `subjectId` to `objectId`.

Returns a Promise for a Boolean `true`.

### query
`.query(type, index)` -- Returns a new [Query class](#query-class) instance.

Query Class
-----------
```JS
const db = DynamoDBEngine.create(config, schema);

const query = db.query('character', 'byName');

query.rangeEqual('Captain America').fetchAll().then(function (records) {
  records.forEach(function (rec) {
    console.log(rec);
  })
});
```

TODO: Document the Query Class.

Events
------
### Listen for Events
```JS
const db = DynamoDBEngine.create(config, schema);

db.log.on('request', function (req) {
  console.log('%s %s %s %s %s', ev.apiCall, ev.operation, ev.method, ev.href, ev.body);
});
```

Events are emitted on the instance `.log` object (a Node.js EventEmitter).

### Event "request"
- .apiCall - The DynamoDB instance method used.
- .operation - The DynamoDB API operation.
- .metho - The HTTP method string.
- .href - The full request URL string.
- .body - The request body string.

Testing
-------
Testing is done using the [XO Linter](https://github.com/sindresorhus/xo) and the [Jasmine](http://jasmine.github.io/) test framework. Tests can be run with

    npm test

The `npm test` command will first run the XO Linter and then run the specs with Jasmine. Test files are located in the `spec/` directory.

Reference
---------
DynamoDB Endpoints: http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region

Copyright and License
---------------------
Copyright: (c) 2015 - 2016 by Odd Networks Inc. (http://oddnetworks.co)

Unless otherwise indicated, all source code is licensed under the MIT license. See MIT-LICENSE for details.
