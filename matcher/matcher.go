package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/naoina/toml"
	"github.com/streadway/amqp"
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
es_workers = 5
`

const xsubExchange = "xsub"
const xsubExchangeType = "headers"
const xpubExchange = "xpub"
const xpubExchangeType = "direct"
const xpubRoutingKey = ""
const queryCacheTTL = 3600
const queryIndexType = "cvst"
const percolatorType = ".percolator"

type worker struct {
	AMQP          string
	Elasticsearch string
	ESWorkers     int `toml:"es_workers"`

	queryTTL    int
	sub, pub    *amqp.Channel
	pubMessages chan pubMessage
	subMessages chan subMessage
	es          *elastic.Client
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

type subMatchMap map[string]map[string][]string
type subMessage struct {
	SubscribersMatch subMatchMap            `json:"subscribersMatch"`
	Data             map[string]interface{} `json:"data"`
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
	w.pubMessages = make(chan pubMessage, w.ESWorkers)
	w.subMessages = make(chan subMessage)

	for i := 0; i < w.ESWorkers; i++ {
		go func() {
			w.matchData(ctx)
		}()
	}

	go func() {
		w.publish(ctx, w.redial(ctx, xsubExchange, xsubExchangeType))
		log.Println("Pub Done")
		done()
	}()

	go func() {
		w.subscribe(w.redial(ctx, xpubExchange, xpubExchangeType))
		log.Println("Sub Done")
		done()
	}()
	<-ctx.Done()
}

// redial continually connects to the URL, exiting the program when no longer possible
func (w *worker) redial(ctx context.Context, exchange string, exchangeType string) chan session {
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

			if err = ch.ExchangeDeclare(exchange, exchangeType, false, false, false, false, nil); err != nil {
				str := fmt.Sprintf("cannot declare exchange: %v, %v, %v", exchange, exchangeType, err)
				log.Println(str)
				sessions <- session{nil, nil, errors.New(str)}
				return
			}

			select {
			case sessions <- session{conn, ch, nil}:
			case <-ctx.Done():
				log.Println("shutting down new session", exchange)
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
		queue, err := sub.QueueDeclare("", false, true, true, false, nil)
		if err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue.Name, err)
			return
		}

		if err := sub.QueueBind(queue.Name, "", xpubExchange, false, nil); err != nil {
			log.Printf("cannot consume without a binding to exchange: %q, %v", xpubExchange, err)
			return
		}

		deliveries, err := sub.Consume(queue.Name, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue.Name, err)
			return
		}

		err = sub.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)

		if err != nil {
			log.Printf("cannot set qos: %q, %v", queue.Name, err)
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
			w.pubMessages <- newPub
		}
	}
}

func (w *worker) matchData(ctx context.Context) {
	for {
		select {
		case publication := <-w.pubMessages:
			pr, err := w.es.Percolate().Doc(publication.Data).Index(publication.Topic).Type(queryIndexType).Do()
			if err != nil {
				log.Println("Percolation failed", err)
				continue
			}
			matchedSubs := make(map[string]map[string]map[string]bool)
			for _, match := range pr.Matches {
				query, err := w.es.Get().Index(match.Index).Id(match.Id).Type(percolatorType).Do()
				if err != nil {
					log.Println("Percolation query fetch failed", err)
					continue
				}
				var sub subscription
				err = json.Unmarshal(*query.Source, &sub)
				if err != nil {
					log.Println("Malformed subscription", err)
					continue
				}
				subscribersMap, ok := matchedSubs[sub.XSUB]
				if !ok {
					subscribersMap = make(map[string]map[string]bool)
					matchedSubs[sub.XSUB] = subscribersMap
				}
				subscriptionsMap, ok := subscribersMap[sub.Subscriber]
				if !ok {
					subscriptionsMap = make(map[string]bool)
					subscribersMap[sub.Subscriber] = subscriptionsMap
				}
				subscriptionsMap[sub.SubID] = true
			}
			matchedSubsList := make(subMatchMap)
			for xsub, subscribersMap := range matchedSubs {
				matchedSubsList[xsub] = make(map[string][]string)
				for subscriber, subscriptionsMap := range subscribersMap {
					subIDs := []string{}
					for subID := range subscriptionsMap {
						subIDs = append(subIDs, subID)
					}
					matchedSubsList[xsub][subscriber] = subIDs
				}
			}
			matchedData := subMessage{
				SubscribersMatch: matchedSubsList,
				Data:             publication.Data,
			}
			w.subMessages <- matchedData
		case <-ctx.Done():
			return
		}
	}

}

func (w *worker) publish(ctx context.Context, sessions chan session) {
	var (
		reading = w.subMessages
		pending = make(chan subMessage, 1)
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
					log.Printf("nack message %d", confirmed.DeliveryTag)
					if confirmed.DeliveryTag == 0 {
						return
					}
				}
				reading = w.subMessages
			case body := <-pending:
				headers := make(amqp.Table)
				for xsub := range body.SubscribersMatch {
					headers[xsub] = int32(1)
				}
				mbody, err := json.Marshal(body)
				if err != nil {
					log.Println("Could not marshal the message", err)
					continue
				}
				routingKey := ""
				err = pub.Publish(xsubExchange, routingKey, false, false, amqp.Publishing{
					Body:         mbody,
					DeliveryMode: amqp.Persistent,
					Headers:      headers,
					ContentType:  "application/octet-stream",
				})
				// Retry failed delivery on the next session
				if err != nil {
					log.Println("Could not publish the message", err)
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
			case <-ctx.Done():
				log.Println("Exiting publihser, context is closed")
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
		ESWorkers:     5,
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
