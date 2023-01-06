package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderProducer struct {
	producer      *kafka.Producer
	topic         string
	delivery_chan chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderProducer {
	return &OrderProducer{
		producer:      p,
		topic:         topic,
		delivery_chan: make(chan kafka.Event, 10000),
	}
}

type Payload struct {
	OrderType string `json:"order_type"`
	Size      int    `json:"size"`
}

func (op *OrderProducer) placeOrder(orderType string, size int) error {
	// format := fmt.Sprintf("%s - %d", orderType, size)
	p := Payload{
		OrderType: orderType,
		Size:      size,
	}
	temp, _ := json.Marshal(&p)
	fmt.Printf("Check values of payload : %+v\n", p)
	err := op.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &op.topic, Partition: kafka.PartitionAny},
		Value:          temp},
		op.delivery_chan,
	)
	fmt.Printf("marshal to string : %s \n", string(temp))
	if err != nil {
		log.Fatal(err)
	}
	<-op.delivery_chan

	return nil

}

func main() {

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "something",
		"acks":              "all"})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)

	}
	topic := "HVSE"

	// go func() {
	// 	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 		"bootstrap.servers": "localhost:9092",
	// 		"group.id":          "foo",
	// 		"auto.offset.reset": "smallest"})

	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}

	// 	err = consumer.Subscribe(topic, nil)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	for {
	// 		ev := consumer.Poll(100)
	// 		switch e := ev.(type) {
	// 		case *kafka.Message:
	// 			result := Payload{}
	// 			json.Unmarshal(e.Value, &result)
	// 			fmt.Printf("Consumed message from the que : %+v\n", result)
	// 		}
	// 	}
	// }()

	op := NewOrderProducer(p, topic)

	for i := 0; i < 1000; i++ {
		if err := op.placeOrder("Jayanth", i+1); err != nil {
			log.Fatal(err)
		}

		time.Sleep(time.Second * 2)
	}

	// delivery_chan := make(chan kafka.Event, 10000)

	// for {
	// 	err = op.producer.Produce(&kafka.Message{
	// 		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
	// 		Value:          []byte("Foo")},
	// 		delivery_chan,
	// 	)
	// 	if err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	<-delivery_chan

	// 	time.Sleep(time.Second * 2)

	// }

}
