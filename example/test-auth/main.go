package main

import (
	"context"
	"fmt"
	"os"

	"cloud.google.com/go/managedkafka/apiv1/managedkafkapb"
	"google.golang.org/api/iterator"

	managedkafka "cloud.google.com/go/managedkafka/apiv1"
)

func main() {
	projectID := os.Getenv("GCP_PROJECT")
	region := os.Getenv("GCP_REGION")
	clusterID := os.Getenv("GCP_CLUSTER_ID")
	ctx := context.Background()
	client, err := managedkafka.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	clusterPath := fmt.Sprintf("projects/%s/locations/%s/clusters/%s", projectID, region, clusterID)
	req := &managedkafkapb.ListTopicsRequest{
		Parent: clusterPath,
	}
	topicIter := client.ListTopics(ctx, req)
	for {
		res, err := topicIter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			panic(err)
		}
		fmt.Printf("Got topic: %v", res)
	}
}
