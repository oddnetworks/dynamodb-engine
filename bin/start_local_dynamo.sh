#!/usr/bin/env bash
bindir="$(cd `dirname "$0"` && pwd)"
rootdir="$( dirname "$THISDIR" )"

dynamodb_path=$1;

java "-Djava.library.path=$dynamodb_path/DynamoDBLocal_lib" -jar "$dynamodb_path/DynamoDBLocal.jar" -dbPath "$rootdir/data/"
