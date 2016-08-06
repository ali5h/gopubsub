package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/naoina/toml"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"gopkg.in/olivere/elastic.v3"
)

const usage = `

Usage:
  matcher <flags>

The flags are:
  -config <file>     configuration file to load
  -sample-config     print out full sample configuration to stdout
`

const sampleConfig = `
amqp = "amqp://guest:guest@localhost:5672/"
elasticsearch = "http://localhost:9200/"
topic = "new_xpub_data"
`
const xsubExchange = "xsub"
const xsubExchangeType = "headers"
const xpubExchange = "xpub"
const xpubExchangeType = "direct"
const xpubRoutingKey = ""
const queryCacheTTL = 3600

type worker struct {
	AMQP          string
	Elasticsearch string
	Topic         string

	queryTTL int
	sub, pub *amqp.Channel
	messages chan pubMessage
	ctx      context.Context
	es       *elastic.Client
}

type session struct {
	*amqp.Connection
	*amqp.Channel
	err error
}

// Close tears the connection down, taking the channel with it.
func (s session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

type pubMessage struct {
	Topic string                 `json:"topicName"`
	Data  map[string]interface{} `json:"data"`
}

type subscription struct {
	SubID      string `json:"sub_id"`
	XSUB       string `json:"xsub"`
	Subscriber string `json:"sub_name"`
}

func (w *worker) init(file string) error {
	if file == "" {
		return errors.New("Please provide a config file")
	}
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	if err := toml.Unmarshal(buf, w); err != nil {
		return err
	}

	client, err := elastic.NewClient(elastic.SetURL(w.Elasticsearch))
	if err != nil {
		log.Printf("No Elasticsearch at %s", w.Elasticsearch)
		return err
	}
	w.es = client

	return nil
}

func (w *worker) start() {
	ctx, done := context.WithCancel(context.Background())
	w.ctx = ctx
	w.messages = make(chan pubMessage)

	go func() {
		w.publish(w.redial(xsubExchange, xsubExchangeType))
		log.Println("Pub Done")
		done()
	}()

	go func() {
		w.subscribe(w.redial(xpubExchange, xpubExchangeType))
		log.Println("Sub Done")
		done()
	}()
	<-ctx.Done()
}

// redial continually connects to the URL, exiting the program when no longer possible
func (w *worker) redial(exchange string, exchangeType string) chan session {
	url := w.AMQP
	sessions := make(chan session, 1)
	go func() {
		defer close(sessions)

		for {
			conn, err := amqp.Dial(url)
			if err != nil {
				str := fmt.Sprintf("cannot (re)dial: %v: %q", err, url)
				log.Println(str)
				sessions <- session{nil, nil, errors.New(str)}
				return
			}

			ch, err := conn.Channel()
			if err != nil {
				str := fmt.Sprintf("cannot create channel: %v", err)
				log.Println(str)
				sessions <- session{nil, nil, errors.New(str)}
				return
			}

			if err = ch.ExchangeDeclare(exchange, exchangeType, false, true, false, false, nil); err != nil {
				str := fmt.Sprintf("cannot declare exchange: %v, %v, %v", exchange, exchangeType, err)
				log.Println(str)
				sessions <- session{nil, nil, errors.New(str)}
				return
			}

			select {
			case sessions <- session{conn, ch, nil}:
			case <-w.ctx.Done():
				log.Println("shutting down new session")
				return
			}
		}
	}()
	return sessions
}

func (w *worker) subscribe(sessions chan session) {

	for sub := range sessions {
		if sub.err != nil {
			return
		}
		if _, err := sub.QueueDeclare(w.Topic, false, true, true, false, nil); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", w.Topic, err)
			return
		}

		if err := sub.QueueBind(w.Topic, xpubRoutingKey, xpubExchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", xpubExchange, err)
			return
		}

		deliveries, err := sub.Consume(w.Topic, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", w.Topic, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			sub.Ack(msg.DeliveryTag, false)
			var newPub pubMessage
			if err := json.Unmarshal(msg.Body, &newPub); err != nil {
				log.Println(err)
				continue
			}
			go w.matchData(newPub)
		}
	}
}

func (w *worker) matchData(publication pubMessage) {
	pr, err := w.es.Percolate().Doc(publication.Data).Index(publication.Topic).Type("cvst").Do()
	if err != nil {
		log.Println("Percolation failed")
		return
	}
	// matchedSubs := make(map[string]map[string][]string)
	for _, match := range pr.Matches {
		log.Println(match.Id)
		query, err := w.es.Get().Index(match.Index).Id(match.Id).Type(".percolator").Do()
		if err != nil {
			log.Println("Percolation query fetch failed")
			continue
		}
		var sub subscription
		err = json.Unmarshal(*query.Source, &sub)
		if err != nil {
			log.Println("Malformed subscription")
			continue
		}
		// matchedSubs[sub.XSUB] :=
	}
	w.messages <- publication
}

func (w *worker) publish(sessions chan session) {
	var (
		reading = w.messages
		pending = make(chan pubMessage, 1)
		confirm = make(chan amqp.Confirmation, 1)
	)

	for pub := range sessions {
		if pub.err != nil {
			return
		}

		// publisher confirms for this channel/connection
		if err := pub.Confirm(false); err != nil {
			log.Printf("publisher confirms not supported")
			close(confirm) // confirms not supported, simulate by always nacking
		} else {
			pub.NotifyPublish(confirm)
		}

		log.Printf("publishing...")

		for {
			select {
			case confirmed := <-confirm:
				if !confirmed.Ack {
					if confirmed.DeliveryTag == 0 {
						return
					}
					log.Printf("nack pubMessage %d", confirmed.DeliveryTag)
				}
				reading = w.messages
			case body := <-pending:
				// headers := amqp.Table{
				// "instance": 1,
				// }
				mbody, err := json.Marshal(body)
				if err != nil {
					log.Printf("Could not marshal the pubMessage")
					continue
				}
				routingKey := ""
				err = pub.Publish(xsubExchange, routingKey, false, false, amqp.Publishing{
					Body:         mbody,
					DeliveryMode: 2,
					// Headers:      headers,
					ContentType: "application/octet-stream",
				})
				// Retry failed delivery on the next session
				if err != nil {
					pending <- body
					pub.Close()
					break
				}

			case body, running := <-reading:
				// all pubMessages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- body
				reading = nil
			case <-w.ctx.Done():
				return
			}
		}
	}
}

func main() {
	var (
		fConfig       = flag.String("config", "", "configuration file to load")
		fSampleConfig = flag.Bool("sample-config", false, "print out full sample configuration")
	)
	flag.Usage = func() { usageExit(0) }
	flag.Parse()

	if *fSampleConfig {
		fmt.Println(sampleConfig)
		return
	}

	w := worker{
		AMQP:          "amqp://guest:guest@localhost:5672/",
		Elasticsearch: "http://localhost:9200/",
		Topic:         "new_xpub_data",
	}

	err := w.init(*fConfig)
	if err != nil {
		log.Fatalln(err)
	}

	log.Println("Starting the system")
	w.start()
}

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}
