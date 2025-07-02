package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/IBM/sarama"
)

const (
	kafkaPort   = "localhost:9092"
	topic       = "test-topic"
	workerCount = 3
)

type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	log.Println("=> Setup: инициализация")
	return nil
}

func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	log.Println("=> Cleanup: завершение")
	return nil
}

func (ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Println("=> ConsumeClaim: старт обработки")

	for msg := range claim.Messages() {
		log.Printf("=> Получено сообщение из группы: %s", string(msg.Value))
		sess.MarkMessage(msg, "")
	}

	return nil
}

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

func consumeMessages() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	groupID := "test-group"

	consumerGroup, err := sarama.NewConsumerGroup([]string{kafkaPort}, groupID, config)
	if err != nil {
		log.Fatalf("Ошибка создания consumer group: %v", err)
	}
	defer consumerGroup.Close()

	ctx := context.Background()

	handler := ConsumerGroupHandler{}

	log.Println("Потребитель вступает в группу...")

	for {
		err := consumerGroup.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Fatalf("Ошибка при чтении: %v", err)
		}
	}
}
