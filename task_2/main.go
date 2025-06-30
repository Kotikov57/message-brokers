package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

const (
	kafkaPort = "localhost:9092"
	topic     = "test-topic"
)

func main() {
	createTopic()

	produceMessages()

	consumeMessages()
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
		for msg := range producer.Successes(){
			log.Printf("%s успешно отправлено в тему %s, offset %d\n",msg.Value, msg.Topic, msg.Offset)
		}
	}()

	go func() {
		for err := range producer.Errors() {
			log.Printf("Ошибка при отправке сообщения: %v", err.Err)
		}
	}()

	for i := 0; i < 10; i++ {
		msg := fmt.Sprintf("Сообщение #%d", i+1)
		producer.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(msg),
		}
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(1 * time.Second)
}

func consumeMessages() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{kafkaPort}, config)
	if err != nil {
		log.Fatalf("Ошибка создания консюмера: %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Ошибка подписки на партицию: %v", err)
	}
	defer partitionConsumer.Close()

	log.Println("Ожидание сообщений...")
	count := 0
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("Получено: %s\n", string(msg.Value))
			count++
			if count >= 10 {
				return
			}
		case err := <-partitionConsumer.Errors():
			log.Printf("Ошибка потребителя: %v", err)
		case <-time.After(10 * time.Second):
			log.Println("Таймаут ожидания сообщений")
			return
		}
	}
}
