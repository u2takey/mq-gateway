package runtime

import (
	"log"

	"github.com/streadway/amqp"
)

// A HandlerFunc handles a specific topic from queue
type HandlerFunc func(r []byte) []byte
type ServeMux struct {
	// handlers maps mq topic method to a list of handlers.
	handlers     map[string]handler
	mqClient     *MqClient
	exchangeName string
	exchangeType string
}

type MqClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	tag     string
	done    chan error
}

// NewServeMux returns a new ServeMux whose internal mapping is empty.
func NewServeMux(amqpURI, exchangeName, exchangeType string) *ServeMux {
	serveMux := &ServeMux{
		exchangeType: exchangeType,
		exchangeName: exchangeName,
		handlers:     make(map[string]handler),
		mqClient:     newMqclient(amqpURI, exchangeName, exchangeType),
	}

	return serveMux
}

func (s *ServeMux) Handle(queueName string, h HandlerFunc) {
	handle := handler{queue: queueName, h: h}
	s.handlers[queueName] = handle

	log.Printf("declaring Queue %q", queueName)
	queue, err := s.mqClient.channel.QueueDeclare(
		queueName, // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange",
		queue.Name, queue.Messages, queue.Consumers)

	queue_out, err := s.mqClient.channel.QueueDeclare(
		queueName+"_out", // name of the queue
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange",
		queue_out.Name, queue_out.Messages, queue_out.Consumers)

	if err = s.mqClient.channel.QueueBind(
		queue.Name,     // name of the queue
		queue.Name,     // bindingKey
		s.exchangeName, // sourceExchange
		false,          // noWait
		nil,            // arguments
	); err != nil {
		log.Panic(err)
	}

	if err = s.mqClient.channel.QueueBind(
		queue.Name+"_out", // name of the queue
		queue.Name+"_out", // bindingKey
		s.exchangeName,    // sourceExchange
		false,             // noWait
		nil,               // arguments
	); err != nil {
		log.Panic(err)
	}

	log.Printf("Starting Consume")
	deliveries, err := s.mqClient.channel.Consume(
		queue.Name, // name
		"",
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		log.Panic(err)
	}

	go s.consume(deliveries, handle)

	return
}

func (s *ServeMux) Start() {

}

type handler struct {
	queue string
	h     HandlerFunc
}

func newMqclient(amqpURI, exchange, exchangeType string) *MqClient {
	c := &MqClient{
		conn:    nil,
		channel: nil,
		tag:     "",
		done:    make(chan error),
	}

	var err error

	log.Printf("dialing %q", amqpURI)
	c.conn, err = amqp.Dial(amqpURI)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		log.Panic(err)
	}

	log.Printf("got Channel, declaring Exchange (%q)", exchange)
	if err = c.channel.ExchangeDeclare(
		exchange,     // name of the exchange
		exchangeType, // type
		true,         // durable
		false,        // delete when complete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	); err != nil {
		log.Panic(err)
	}
	return c
}

func (s *ServeMux) consume(deliveries <-chan amqp.Delivery, handle handler) {
	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		out := handle.h(d.Body)
		s.publish(handle.queue, out)
		_ = d.Ack(false)
	}
	log.Printf("handle: deliveries channel closed")
}

func (s *ServeMux) publish(queueName string, body []byte) {

	log.Printf("publishing %dB body (%q)", len(body), string(body))
	if err := s.mqClient.channel.Publish(
		s.exchangeName,   // publish to an exchange
		queueName+"_out", // routing to 0 or more queues
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		log.Panic(err)
	}

	return
}
