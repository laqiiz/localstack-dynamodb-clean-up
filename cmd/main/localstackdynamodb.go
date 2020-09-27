package main

import (
	"context"
	"fmt"
	"github.com/laqiiz/localstack-dynamodb-clean-up"
	"log"
	"os"
	"strings"
)

func main() {
	tables := os.Args[1:]

	inputs := make([]string, 0, len(tables))
	for _, table := range tables {
		if len(tables) == 0 {
			continue
		}
		inputs = append(inputs, strings.TrimSpace(table))
	}

	if err := localstackdynamodb.CleanUpAll(context.Background(), tables); err != nil {
		log.Fatal(err)
	}
	fmt.Println("success")
}
