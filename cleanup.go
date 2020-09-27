package localstackdynamodb

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/sync/errgroup"
	"strings"
)

var db = dynamodb.New(session.Must(session.NewSession(&aws.Config{
	Endpoint: aws.String("http://localhost:4566"),
	Region:   aws.String(endpoints.ApNortheast1RegionID),
})))

func CleanUpAll(ctx context.Context, tables []string) error {
	eg := errgroup.Group{}
	for _, table := range tables {
		table := table
		eg.Go(func() error {
			if err := CleanUp(ctx, table); err != nil {
				return fmt.Errorf("clean %s: %w",table, err)
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("celan up all: %w", err)
	}
	return nil
}

func CleanUp(ctx context.Context, table string) error {
	var startKey map[string]*dynamodb.AttributeValue

	keys, err := getKeys(ctx, table)
	if err != nil {
		return fmt.Errorf("get table keys: %w", err)
	}

	for {
		items, sk, err := scan(ctx, table, keys, startKey)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		if err := batchDelete(ctx, table, items); err != nil {
			return fmt.Errorf("batch delete: %w", err)
		}

		startKey = sk
		if len(startKey) == 0 {
			break // 続きが無ければ終了
		}
	}
	return nil
}

func getKeys(ctx context.Context, table string) ([]string, error) {
	desc, err := db.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(desc.Table.KeySchema))
	for _, schema := range desc.Table.KeySchema {
		keys = append(keys, *schema.AttributeName)
	}
	return keys, nil
}

func scan(ctx context.Context, table string, keys []string, startKey map[string]*dynamodb.AttributeValue) ([]map[string]*dynamodb.AttributeValue, map[string]*dynamodb.AttributeValue, error) {

	expressionAttributeNames := make(map[string]*string, len(keys))
	projectKeys := make([]string, 0, len(keys))
	for _, v := range keys {
		expressionAttributeNames["#"+v] = aws.String(v)
		projectKeys = append(projectKeys, "#"+v)
	}

	out, err := db.ScanWithContext(ctx, &dynamodb.ScanInput{
		ExclusiveStartKey:        startKey,
		ExpressionAttributeNames: expressionAttributeNames,
		ProjectionExpression:     aws.String(strings.Join(projectKeys, ",")),
		TableName:                aws.String(table),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("scan %s %w", table, err)
	}
	return out.Items, out.LastEvaluatedKey, nil
}

func batchDelete(ctx context.Context, tableName string, deletes []map[string]*dynamodb.AttributeValue) error {
	var items []*dynamodb.WriteRequest
	for _, v := range deletes {
		items = append(items, &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: v,
			},
		})

		if len(items) >= 25 {
			out, err := db.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
				RequestItems: map[string][]*dynamodb.WriteRequest{
					tableName: items,
				},
			})

			if err != nil {
				return err
			}
			// 書き込みが成功したらリクエストを初期化
			items = items[:0]

			// 未処理のitemsがあれば再設定
			remain := out.UnprocessedItems[tableName]
			if len(remain) > 0 {
				items = append(items, remain...)
			}
		}
	}

	if len(items) > 0 {
		out, err := db.BatchWriteItemWithContext(ctx, &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: items,
			},
		})
		if err != nil {
			return err
		}
		// 書き込みが成功したらリクエストを初期化
		items = items[:0]

		// 未処理のitemsがあれば再設定
		remain := out.UnprocessedItems[tableName]
		if len(remain) > 0 {
			items = append(items, remain...)
		}
	}

	return nil
}
