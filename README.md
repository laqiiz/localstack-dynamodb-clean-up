# localstack-dynamodb-clean-up
Go CLI tool to clean up for dynamodb on localstack.

## Abstract

Delete All Item of DynamoDB-Local on LocalStack that using 4566 port.
This tool operates bellow flow.

1. Describe table and get key schema.
2. Scan all items.
3. Batch write items(using dynamodb delete request)

## Installation

Build from source.

```
go get -u github.com/laqiiz/localstack-dynamodb-clean-up/cmd/localstackdynamodbcleanup
```

## Usage

```
./localstackdynamodbcleanup <table name>
```

## Excample

```
./localstackdynamodbcleanup local_table1 local_table2
```

