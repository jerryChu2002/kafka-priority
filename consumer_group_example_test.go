package sarama

import (
	"context"
	"fmt"
)

type exampleConsumerGroupHandler struct{}

func (exampleConsumerGroupHandler) Setup(_ ConsumerGroupSession) error   { return nil }
func (exampleConsumerGroupHandler) Cleanup(_ ConsumerGroupSession) error { return nil }
func (h exampleConsumerGroupHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg, "")
	}
	return nil
}
func (h exampleConsumerGroupHandler) ConsumeClaimsWithPriority(cgs ConsumerGroupSession, cgcs []ConsumerGroupClaim) error {
	for _, cgc := range cgcs {
		for {
			select {
			case msg, ok := <-cgc.Messages():
				if !ok {
					// 如果通道关闭，跳出循环
					fmt.Printf("No more messages in topic:%q partition:%d, moving to next claim.\n", cgc.Topic(), cgc.Partition())
					break
				}
				fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
				cgs.MarkMessage(msg, "")
			default:
				// 没有消息时，跳到下一个 ConsumerGroupClaim
				fmt.Printf("No messages in topic:%q partition:%d, moving to next claim.\n", cgc.Topic(), cgc.Partition())
				break
			}
		}
	}
	return nil
}

func (h exampleConsumerGroupHandler) SortClaimsWithPriority(cgs []ConsumerGroupClaim) []ConsumerGroupClaim {
	//TODO 根据已知的topic做优先级排序即可，到ConsumeClaimsWithPriority时先消费排序靠前的claim
	return nil
}

func (h exampleConsumerGroupHandler) IsPriorityConsumer() bool {
	return true
}
func ExampleConsumerGroup() {
	config := NewTestConfig()
	config.Version = V2_0_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true

	group, err := NewConsumerGroup([]string{"localhost:9092"}, "my-group", config)
	if err != nil {
		panic(err)
	}
	defer func() { _ = group.Close() }()

	// Track errors
	go func() {
		for err := range group.Errors() {
			fmt.Println("ERROR", err)
		}
	}()

	// Iterate over consumer sessions.
	ctx := context.Background()
	for {
		topics := []string{"my-topic"}
		handler := exampleConsumerGroupHandler{}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		err := group.Consume(ctx, topics, handler)
		if err != nil {
			panic(err)
		}
	}
}
