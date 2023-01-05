package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "something",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)

	}

	delivery_chan := make(chan kafka.Event, 10000)
	topic:= "HVSE"
	err = p.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
    Value: []byte("Foo")},
    delivery_chan,
)
	if err!=nil{
		log.Fatal(err)
	}
	e:= <-delivery_chan

	f
	fmt.Printf("%+v/n", p)

}
