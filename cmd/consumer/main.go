package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "introducao-kafka_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest",
	}
	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		log.Println("Error consumer", err.Error())
	}

	topics := []string{"teste2"}
	c.SubscribeTopics(topics, nil)
	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}

	}
}
