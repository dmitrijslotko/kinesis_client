package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}
	kinesisClient := kinesis.NewFromConfig(cfg)
	consumerName := "my-consumer"
	streamARN := "REPLACE_WITH_STREAM_ARN"

	consumerARN := createSubscription(kinesisClient, &kinesis.RegisterStreamConsumerInput{
		ConsumerName: &consumerName,
		StreamARN:    &streamARN,
	})

	read(kinesisClient, consumerARN, &streamARN)

}

func read(client *kinesis.Client, consumerARN *string, streamARN *string) {
	for { 

		shards, err := listShards(client, &kinesis.ListShardsInput{
			StreamARN: streamARN,
		})

		if err != nil {
			log.Println("Error listing shards, retrying in 30 seconds")
			time.Sleep(1 * time.Second)
			continue 
		}

		var wg sync.WaitGroup

		for _, shard := range *shards {
			wg.Add(1)
			go subscribeToShard(client, consumerARN, shard, &wg)
		}

		wg.Wait()

	}
}

func createSubscription(client *kinesis.Client, params *kinesis.RegisterStreamConsumerInput) *string {
	ctx := context.TODO()

	output, err := client.RegisterStreamConsumer(ctx, params)
	if err != nil {
		fmt.Printf("Failed to register consumer: %v\n", err)
		if strings.Contains(err.Error(), "ResourceInUseException") {
			fmt.Printf("Consumer already exists")

			describeOutput, describeErr := client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
				ConsumerName: params.ConsumerName,
				StreamARN:    params.StreamARN,
			})

			if describeErr != nil {
				log.Printf("Failed to describe existing consumer: %v", describeErr)
				return nil
			}

			return describeOutput.ConsumerDescription.ConsumerARN
		} else {
			log.Printf("Failed to register consumer: %v", err)
			return nil
		}
	}

	for {
		consumerStatus, err := client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
			ConsumerARN: output.Consumer.ConsumerARN,
			StreamARN:   params.StreamARN,
		})
		if err != nil {
			log.Printf("Failed to describe consumer: %v", err)
			return nil
		}

		if consumerStatus.ConsumerDescription.ConsumerStatus == types.ConsumerStatusActive {
			fmt.Println("Consumer is active")
			break
		} else {
			time.Sleep(time.Second)
		}
	}

	return output.Consumer.ConsumerARN
}


func listShards(client *kinesis.Client, params *kinesis.ListShardsInput) (*[]types.Shard, error) {
	output, err := client.ListShards(context.TODO(), params)
	if err != nil {
		log.Printf("failed to list shards: %v", err)
		return nil, err
	}
	return &output.Shards, nil
}

func subscribeToShard(client *kinesis.Client, consumerARN *string, shard types.Shard, wg *sync.WaitGroup) {
	defer wg.Done()

	subscribeOutput, err := client.SubscribeToShard(context.TODO(), &kinesis.SubscribeToShardInput{ // Use the context here
		ConsumerARN: consumerARN,
		ShardId:     shard.ShardId,
		StartingPosition: &types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		},
	})
	if err != nil {
		log.Printf("failed to subscribe to shard %s: %v", *shard.ShardId, err)
		return
	}

	stream := subscribeOutput.GetStream()

	for event := range stream.Events() {
		if recordEvent, ok := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
			seconds := *recordEvent.Value.MillisBehindLatest / 1000
			fmt.Printf("Shard %s: Seconds Behind: %d\n", *shard.ShardId, seconds)
		}
	}

	if err := stream.Err(); err != nil {
		log.Printf("error in event stream for shard %s: %v", *shard.ShardId, err)
	}
}
