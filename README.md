DynamoDB Engine
===============
A promisified Node.js engine for AWS DynamoDB.

DynamoDB Endpoints: http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region

Testing
-------
### Integration Testing
Integration tests also use [tape](https://github.com/substack/tape), and can be found in `integration-test/`. There is a fixtures folder (`integration-test/fixtures/`) where fixtures are commited to source code for testing.

Integration tests can be run against a local instance of DynamoDB, or against a remote DynamoDB endpoint. To run locally you'll need to install the [local DynamoDB server](http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html). A good place to install it on a Unix system would be `/opt/dynamodb/`. Once you have it installed, there is a script for starting it in `bin/start_local_dynamo.sh`. You pass it the directory where you installed DynamoDB locally like this:

    ./bin/start_local_dynamo.sh /opt/dynamodb/

That will set DynamoDB up to write database files to the project `data/` directory.

!WARNING: Make sure your current user has access to the directory where you installed DynamoDB locally.

Before you can run the tests, you'll need to export these environment variables, even if you're running locally:

- `ACCESS_KEY_ID` - Your AWS access key ID.
- `SECRET_ACCESS_KEY` - Your AWS secret access key.
- `REGION` - The AWS region you want to use (http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region).

Then run

    npm run integration-test

You can also run the integration tests against a real DynamoDB endpoint by specifying the ENDPOINT environment variable (http://docs.aws.amazon.com/general/latest/gr/rande.html#ddb_region).

Copyright and License
---------------------
Copyright: (c) 2015 - 2016 by Odd Networks Inc. (http://oddnetworks.co)

Unless otherwise indicated, all source code is licensed under the MIT license. See MIT-LICENSE for details.
