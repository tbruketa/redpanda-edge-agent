package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	log "github.com/sirupsen/logrus"
)

type Redpanda struct {
	name   string
	prefix Prefix
	topics []Topic
	client *kgo.Client
	adm    *kadm.Client
}

var (
	source          Redpanda
	sourceOnce      sync.Once
	destination     Redpanda
	destinationOnce sync.Once
	wg              sync.WaitGroup
)

// Closes the source and destination client connections
func shutdown() {
	log.Infoln("Closing client connections")
	source.adm.Close()
	source.client.Close()
	destination.adm.Close()
	destination.client.Close()
}

// Creates new Kafka and Admin clients to communicate with a cluster.
//
// The `prefix` must be set to either `Source` or `Destination` as it
// determines what settings are read from the configuration.
//
// The topics listed in `source.topics` are the topics that will be pushed by
// the agent from the source cluster to the destination cluster.
//
// The topics listed in `destination.topics` are the topics that will be pulled
// by the agent from the destination cluster to the source cluster.
func initClient(rp *Redpanda, mutex *sync.Once, prefix Prefix) {
	mutex.Do(func() {
		var err error
		name := config.String(
			fmt.Sprintf("%s.name", prefix))
		servers := config.String(
			fmt.Sprintf("%s.bootstrap_servers", prefix))

		topics := GetTopics(prefix)
		var consumeTopics []string
		for _, t := range topics {
			consumeTopics = append(consumeTopics, t.consumeFrom())
			log.Infof("Added %s topic: %s", t.direction.String(), t.String())
		}

		group := config.String(
			fmt.Sprintf("%s.consumer_group_id", prefix))

		opts := []kgo.Opt{}
		opts = append(opts,
			kgo.SeedBrokers(strings.Split(servers, ",")...),
			// https://github.com/redpanda-data/redpanda/issues/8546
			kgo.ProducerBatchCompression(kgo.NoCompression()),
		)
		if len(topics) > 0 {
			opts = append(opts,
				kgo.ConsumeTopics(consumeTopics...),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.SessionTimeout(60000*time.Millisecond),
				kgo.DisableAutoCommit(),
				kgo.BlockRebalanceOnPoll())
		}
		maxVersionPath := fmt.Sprintf("%s.max_version", prefix)
		if config.Exists(maxVersionPath) {
			opts = MaxVersionOpt(config.String(maxVersionPath), opts)
		}
		tlsPath := fmt.Sprintf("%s.tls", prefix)
		if config.Exists(tlsPath) {
			tlsConfig := TLSConfig{}
			config.Unmarshal(tlsPath, &tlsConfig)
			opts = TLSOpt(&tlsConfig, opts)
		}
		saslPath := fmt.Sprintf("%s.sasl", prefix)
		if config.Exists(saslPath) {
			saslConfig := SASLConfig{}
			config.Unmarshal(saslPath, &saslConfig)
			opts = SASLOpt(&saslConfig, opts)
		}

		rp.name = name
		rp.prefix = prefix
		rp.topics = topics
		rp.client, err = kgo.NewClient(opts...)
		if err != nil {
			log.Fatalf("Unable to load client: %v", err)
		}
		// Check connectivity to cluster
		if err = rp.client.Ping(context.Background()); err != nil {
			log.Errorf("Unable to ping %s cluster: %s",
				prefix, err.Error())
		}

		rp.adm = kadm.NewClient(rp.client)
		brokers, err := rp.adm.ListBrokers(context.Background())
		if err != nil {
			log.Errorf("Unable to list brokers: %v", err)
		}
		log.Infof("Created %s client", name)
		for _, broker := range brokers {
			brokerJson, _ := json.Marshal(broker)
			log.Debugf("%s broker: %s", prefix, string(brokerJson))
		}
	})
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

// Check the topics exist on the given cluster. If the topics to not exist then
// this function will attempt to create them if configured to do so.
func checkTopics(cluster *Redpanda) {
	ctx := context.Background()
	createTopics := make(map[string]Topic)
	for _, topic := range AllTopics() {
		if cluster.prefix == Source {
			_, exists := createTopics[topic.sourceName]
			if !exists {
				createTopics[topic.sourceName] = topic
			}
		} else if cluster.prefix == Destination {
			_, exists := createTopics[topic.destinationName]
			if !exists {
				createTopics[topic.destinationName] = topic
			}
		}
	}

	topicDetails, err := cluster.adm.ListTopics(ctx)
	if err != nil {
		log.Errorf("Unable to list topics on %s: %v", cluster.name, err)
		return
	}

	for topicName, topic := range createTopics {
		if !topicDetails.Has(topicName) {
			if config.Exists("create_topics") {
				resp, _ := cluster.adm.CreateTopics(ctx, int32(topic.destinationPartitions), int16(topic.destinationReplicas), nil, topicName)
				for _, ctr := range resp {
					if ctr.Err != nil {
						log.Warnf("Unable to create topic '%s' on %s: %s",
							ctr.Topic, cluster.name, ctr.Err)
					} else {
						log.Infof("Created topic '%s' on %s",
							ctr.Topic, cluster.name)
					}
				}
			} else {
				log.Fatalf("Topic '%s' does not exist on %s",
					topic, cluster.name)
			}
		} else {
			log.Infof("Topic '%s' already exists on %s",
				topic, cluster.name)
		}
	}
}

// Pauses fetching new records when a fetch error is received.
// The backoff period is determined by the number of sequential
// fetch errors received, and it increases exponentially up to
// a maximum number of seconds set by 'maxBackoffSec'.
//
// For example:
//
//	2 fetch errors = 2 ^ 2 = 4 second backoff
//	3 fetch errors = 3 ^ 2 = 9 second backoff
//	4 fetch errors = 4 ^ 2 = 16 second backoff
func backoff(exp *int) {
	*exp += 1
	backoff := math.Pow(float64(*exp), 2)
	if backoff >= config.Float64("max_backoff_secs") {
		backoff = config.Float64("max_backoff_secs")
	}
	log.Warnf("Backing off for %d seconds", int(backoff))
	time.Sleep(time.Duration(backoff) * time.Second)
}

// Log with additional "id" field to identify whether the log message is coming
// from the push routine, or the pull routine.
func logWithId(lvl string, id string, msg string) {
	level, _ := log.ParseLevel(lvl)
	switch level {
	case log.ErrorLevel:
		log.WithField("id", id).Errorln(msg)
	case log.WarnLevel:
		log.WithField("id", id).Warnln(msg)
	case log.InfoLevel:
		log.WithField("id", id).Infoln(msg)
	case log.DebugLevel:
		log.WithField("id", id).Debugln(msg)
	case log.TraceLevel:
		log.WithField("id", id).Traceln(msg)
	}
}

// Continuously fetch batches of records from the `src` cluster and forward
// them to the `dst` cluster. Consumer offsets are only committed when the
// `dst` cluster acknowledges the records.
func forwardRecords(src *Redpanda, dst *Redpanda, ctx context.Context) {
	defer wg.Done()
	var errCount int = 0
	var fetches kgo.Fetches
	var sent bool
	var committed bool
	logWithId("info", src.name,
		fmt.Sprintf("Forwarding records from '%s' to '%s'", src.name, dst.name))

	topicMap := make(map[string]string)
	partitionMap := make(map[string]int)
	for _, t := range AllTopics() {
		topicMap[t.consumeFrom()] = t.produceTo()
		if t.customPartitioning {
			partitionMap[t.produceTo()] = t.destinationPartitions
		}
	}

	for {
		if (sent && committed) || len(fetches.Records()) == 0 {
			// Only poll when the previous fetches were successfully
			// sent and committed
			logWithId("debug", src.name, "Polling for records...")
			fetches = src.client.PollRecords(
				ctx, config.Int("max_poll_records"))
			if errs := fetches.Errors(); len(errs) > 0 {
				for _, e := range errs {
					if e.Err == context.Canceled {
						logWithId("info", src.name,
							fmt.Sprintf("Received interrupt: %s", e.Err))
						return
					}
					logWithId("error", src.name,
						fmt.Sprintf("Fetch error: %s", e.Err))
				}
				backoff(&errCount)
			}
			if len(fetches.Records()) > 0 {
				logWithId("debug", src.name,
					fmt.Sprintf("Consumed %d records", len(fetches.Records())))
				iter := fetches.RecordIter()
				for !iter.Done() {
					record := iter.Next()
					// Change the topic name if necessary
					changeName := topicMap[record.Topic]
					if changeName != "" {
						logWithId("trace", src.name,
							fmt.Sprintf("Mapping topic name '%s' to '%s'",
								record.Topic, changeName))
						record.Topic = changeName
					}
					// If the record key is empty, then set it to the agent id to
					// route the records to the same topic partition.
					if record.Key == nil {
						record.Key = []byte(config.String("id"))
					}
				}
				sent = false
				committed = false
			} else {
				// No records, skip iteration and poll for more records
				continue
			}
		}

		if !sent && !committed {
			//Calculate Partition using murmur hash on key mod # of partitions
			// Send records to destination
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				//Calculate Partition using murmur hash on key mod # of partitions
				partitionCount, exists := partitionMap[record.Topic]
				if exists {
					partition := Murmur2Partition(record.Key, int32(partitionCount))
					record.Partition = partition
				}
				err := dst.client.ProduceSync(
					ctx, record).FirstErr()
				if err != nil {
					if err == context.Canceled {
						logWithId("info", src.name,
							fmt.Sprintf("Received interrupt: %s", err.Error()))
						return
					}
					logWithId("error", src.name,
						fmt.Sprintf("Unable to send %d record:%s,%d to %s: %s", 1, record.Topic, record.Partition, dst.name, err.Error()))
					backoff(&errCount)
				} else {
					sent = true
					logWithId("debug", src.name,
						fmt.Sprintf("Sent %d records to %s", 1, dst.name))
				}
			}
		}

		if sent && !committed {
			// Records have been sent successfully, so commit offsets
			if log.GetLevel() == log.TraceLevel {
				offsets := src.client.UncommittedOffsets()
				offsetsJson, _ := json.Marshal(offsets)
				logWithId("trace", src.name,
					fmt.Sprintf("Committing offsets: %s", string(offsetsJson)))
			}
			err := src.client.CommitUncommittedOffsets(ctx)
			if err != nil {
				if err == context.Canceled {
					logWithId("info", src.name,
						fmt.Sprintf("Received interrupt: %s", err.Error()))
					return
				}
				logWithId("error", src.name,
					fmt.Sprintf("Unable to commit offsets: %s", err.Error()))
				backoff(&errCount)
			} else {
				errCount = 0 // Reset error counter
				committed = true
				logWithId("debug", src.name, "Offsets committed")
			}
		}

		src.client.AllowRebalance()
	}
}

func main() {
	configFile := flag.String(
		"config", "agent.yaml", "path to agent config file")
	logLevelStr := flag.String(
		"loglevel", "info", "logging level")
	flag.Parse()

	logLevel, _ := log.ParseLevel(*logLevelStr)
	log.SetLevel(logLevel)

	InitConfig(configFile)
	initClient(&source, &sourceOnce, Source)
	initClient(&destination, &destinationOnce, Destination)

	checkTopics(&source)
	checkTopics(&destination)

	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, os.Kill)
	if len(source.topics) > 0 {
		wg.Add(1)
		go forwardRecords(&source, &destination, ctx) // Push to destination
	}
	if len(destination.topics) > 0 {
		wg.Add(1)
		go forwardRecords(&destination, &source, ctx) // Pull from destination
	}
	wg.Wait()

	ctx.Done()
	stop()
	shutdown()
	log.Infoln("Agent stopped")
}

func Murmur2Partition(bytes []byte, numPartitions int32) int32 {
	hash := MurmurHash2(bytes)
	partition := positive(hash) % numPartitions
	return partition
}

// From https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L728
func positive(v int32) int32 {
	return v & 0x7fffffff
}

func MurmurHash2(data []byte) (h int32) {
	const (
		M = 0x5bd1e995
		R = 24
		// From https://github.com/apache/kafka/blob/0.10.1/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L342
		seed = int32(-1756908916)
	)

	var k int32

	h = seed ^ int32(len(data))

	// Mix 4 bytes at a time into the hash
	for l := len(data); l >= 4; l -= 4 {
		k = int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
		k *= M
		k ^= int32(uint32(k) >> R) // To match Kafka Impl
		k *= M
		h *= M
		h ^= k
		data = data[4:]
	}

	// Handle the last few bytes of the input array
	switch len(data) {
	case 3:
		h ^= int32(data[2]) << 16
		fallthrough
	case 2:
		h ^= int32(data[1]) << 8
		fallthrough
	case 1:
		h ^= int32(data[0])
		h *= M
	}

	// Do a few final mixes of the hash to ensure the last few bytes are well incorporated
	h ^= int32(uint32(h) >> 13)
	h *= M
	h ^= int32(uint32(h) >> 15)

	return
}
