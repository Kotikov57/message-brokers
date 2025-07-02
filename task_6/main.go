package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

const (
	kafkaPort = "localhost:9092"
	topic     = "test-topic"
)

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	createTopic()

	produceMessages()

	consumer, err := sarama.NewConsumer([]string{kafkaPort}, config)
	if err != nil {
		log.Fatalf("Ошибка создания consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalf("Ошибка закрытия consumer: %v", err)
		}
	}()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Ошибка создания partition consumer: %v", err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalf("Ошибка закрытия partition consumer: %v", err)
		}
	}()

	log.Printf("Консольный потребитель начал чтение из темы %s (partition 0)\n", topic)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("=> Сообщение: offset=%d, partition=%d, key=%s, value=%s",
				msg.Offset, msg.Partition, string(msg.Key), string(msg.Value))

		case err := <-partitionConsumer.Errors():
			log.Printf("Ошибка при чтении сообщения: %v", err)
		}
	}
}

func createTopic() {
	config := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin([]string{kafkaPort}, config)
	if err != nil {
		log.Fatalf("Ошибка создания админ-кластера: %v", err)
	}
	defer admin.Close()

	err = admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && !isTopicAlreadyExists(err) {
		log.Fatalf("Ошибка создания темы: %v", err)
	}

	log.Println("Тема создана (или уже существует)")
}

func isTopicAlreadyExists(err error) bool {
	e, ok := err.(*sarama.TopicError)
	return ok && e.Err == sarama.ErrTopicAlreadyExists
}

func produceMessages() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer([]string{kafkaPort}, config)
	if err != nil {
		log.Fatalf("Ошибка создания продюсера: %v", err)
	}
	defer producer.Close()

	go func() {
		for msg := range producer.Successes() {
			key, _ := msg.Key.Encode()
			log.Printf("%s с ключом %s успешно отправлено в тему %s, offset %d\n", msg.Value, string(key), msg.Topic, msg.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Ошибка при отправке сообщения: %v", err.Err)
		}
	}()

	for i := 0; i < 10; i++ {
		keyNum := rand.Intn(3)
		key := fmt.Sprintf("key-%d", keyNum)
		msg := fmt.Sprintf("Сообщение #%d c ключом %s", i+1, key)
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(key),
			Value: sarama.StringEncoder(msg),
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}
