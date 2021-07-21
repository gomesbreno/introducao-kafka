package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferiu", "teste2", producer, []byte("transferencia"), deliveryChan)
	go DeliveryReport(deliveryChan) // async
	producer.Flush(10000)

	/*	// sync
		e := <-deliveryChan
		msg := e.(*kafka.Message)
		if msg.TopicPartition.Error != nil {
			fmt.Println("Erro ao enviar")
		} else {
			fmt.Println("Mensagem enviada: ", msg.TopicPartition)
		}
		producer.Flush(1000)
	*/
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "introducao-kafka_kafka_1:9092",
		// "delivery.timeout.ms": "10000", // 0 quer dizer que aguarda infinitamente o retorno do recebimento da menssagem
		"acks":               "all",  // 0, 1 ou all (0: não oespera receber o retorno de confirmação de entrega da msg, 1: aguarda pelo menos o lider dizer que persistiu a menssagem, all: aguarda que todos os brokers tenha sincronizado a menssagem)
		"enable.idempotence": "true", // false, não garante a ordem de entrega, true garante (acks obrigatoriamente precisa ser "all" se o parametro for "true")

	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
				// anotar no banco de dados que a mensagem foi processada.
				// ex: confirma que uma transação bancária ocorreu.
			}
		}
	}
}
