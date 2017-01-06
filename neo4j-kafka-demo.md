##Neo4j GraphGist - Enterprise Architectures: Real-time Graph Updates using Kafka Messaging

# Neo4j Use Case: Low Latency Graph Analytics & OLTP

## Introduction

Let's imagine we're a big film studio that's just released an awesome new movie for distribution. We want to ingest the daily box office receipts for 150,000 locations where our movie is being shown.

We have a Neo4j graph that represents the hierarchy of Cinema locations for each of the many distributors we work with.  Each (:Cinema) node has an (:Account) node. We want to update the (:Account) node by attaching a (:DailyBoxOffice) node that stores the daily revenue.

<img width="888" alt="cinema" src="https://cloud.githubusercontent.com/assets/5991751/21710956/d2642bbe-d3a0-11e6-8706-7d7a145a97a1.png">


The conventional way to perform updates would be use LOAD CSV or perhaps pull data from an API.

In this Gist, I'll demonstrate how to use Apache Kafka to stream continuous message updates to our Neo4j Cinema graph using the Bolt driver.

### Kafka Distributed Streaming Platform

https://kafka.apache.org/

Kakfa allows you to create a resilient messaging service that is fault-tolerant, scalable, and features massively parallel distributed processing.

Kafka was originally developed at LinkedIn, and is becoming widely adopted because it excels at moving large amounts of data quickly across the enterprise.

Using Kafka, LinkedIn has ingested over a trillion messages per day, while Netflix reports ingesting over 500B messages per day on AWS.

<img width="888" alt="netflix" src = "https://cloud.githubusercontent.com/assets/5991751/21710643/f24c3cac-d39e-11e6-8372-5b9cefb596e5.png">


https://techbeacon.com/what-apache-kafka-why-it-so-popular-should-you-use-it

The core of Kafka is the message log, which is essentially a time-dependent data table.  Messages are identified only by their offset in the log, and the log represents a running record of events published by source systems. Applications can subscribe to message streams, allowing for loosely coupled, flexible architectures.

In a production system, the Kafka producers and consumers would be running continuously, with the producers polling for changes in the source data, and the consumers polling to see if there are new messages in the Kafka log for updates.

There are many common scenarios where the lowest possible latency between source systems and Neo4j is desirable (e.g. recommendations, e-commerce, fraud detection, access control, supply chain).  Kafka + Neo4j is a great fit for these use cases.

### Neo4j Kafka Demo

In this demo, we'll spin up a Kafka instance on your local machine.  We'll then use a simple producer client to publish messages to Kafka (mocking an ETL from a source system). Next we'll use a simple consumer to subscribe to Kakfa and send batched message updates to Neo4j using the high-speed Bolt protocol.

The confluent-kafka client is currently the fastest python client available, per recent benchmarking by the Activision data sciences team:

http://activisiongamescience.github.io/2016/06/15/Kafka-Client-Benchmarking/

I've used the Activision benchmarking script as the framework for this demo -- the main modifications I've made are to generate more realistic messages in the Kafak producer, and integrate the Bolt driver with the Kafka consumer.


# Running Kafka on localhost

The easiest way to get started with Kafka is to follow the directions provided on the Confluent Quickstart for setting up a single instance Kafka server.


### Step 1. Java version
Make sure you are running the 1.8 JDK (1.7 works too)

```
$ java -version
java version "1.8.0_102"
Java(TM) SE Runtime Environment (build 1.8.0_102-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.102-b14, mixed mode)
```

### Step 2. Download & Install Confluent Enterprise
Unzip and install the Confluent 3.1.1 on your local machine.  

Enterprise version is fun to play with.

http://docs.confluent.io/3.1.1/installation.html#installation


### Step 3. Install librdkafka library

Install the librdkafka C library, Kafka won't run without it

```
$ brew install librdkafka
```

or install from source:

https://github.com/edenhill/librdkafka

Some useful documentation on configs (pay attention to buffer sizes)

http://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

### Step 4. Confluent Kafka Quickstart - verify services

Now follow the Confluent Quickstart and make sure that you can start Zookeeper and Kafka:

http://docs.confluent.io/3.1.1/quickstart.html#quickstart

When you are done, shut down all of the services in the reverse order that you started them.


# Neo4j Kafka Demo Dependencies

### Step 1. Start Zookeeper and Kafka

To run the demo, you need to open a terminal window and start zookeeper.  
We don't need any of the other Confluent services (schema registry, streams, console, RESTapi).

```
$ ./bin/zookeeper-server-start ./etc/kafka/zookeeper.properties
```

Then open a NEW terminal window and start Kafka

```
$ ./bin/kafka-server-start ./etc/kafka/server.properties
```

If you haven't previously created a Kafka topic, you may need to create one to initialize the server.

```
$ ./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test_topic
```

We'll create another topic later inside the script.

### Step 2. Install Python dependencies.

You'll need to be running Python 3.5 and Jupyter if you want to run the notebook.

If you are new to Python, check out the Anaconda distribution.

```
$ pip install confluent-kafka
$ pip install pykakfa
$ pip install neo4j-driver

```

https://github.com/confluentinc/confluent-kafka-python

https://github.com/Parsely/pykafka

https://github.com/neo4j/neo4j-python-driver

### Step 3. Install Neo4j & APOC dependencies

I'm using Neo4j 3.1 released in Dec 2016.  If you haven't seen this latest version, it's really worth checking out, lots of new enterprise features.  APOC is a huge collection of really useful Neo4j utilities, we'll use some here.

You can get Neo4j here, get the Enterprise versions.

https://neo4j.com/download/

When you start up Neo4j for the first time, you'll be asked to change your password.

```
$ ./bin/neo4j start
Starting Neo4j.
Started neo4j (pid 69178). By default, it is available at http://localhost:7474/
There may be a short delay until the server is ready.

```

You can put in a dummy password "test" and then set it back to the default password "neo4j".
Use this command in the Neo4j browser to get the password prompts:

```
:server change-password
```

Now shutdown Neo4j and install the APOC package .jar file in the neo4j /plugins folder

```
$ ./bin/neo4j stop
Stopping Neo4j.. stopped

```

https://github.com/neo4j-contrib/neo4j-apoc-procedures


When this is done, restart Neo4j.

```
$ ./bin/neo4j start
Starting Neo4j.
Started neo4j (pid 69456). By default, it is available at http://localhost:7474/
There may be a short delay until the server is ready.

```

# Let's do this!

Let's start by making the Cinema graph. You'll need to start with a new, blank Neo4j database.

To achieve good performance, we'll set constraints and indexes on all id fields we'll use for matching update records.

```
# Make sure apoc procedures are installed in Neo4j plugins folder

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError
from string import Template


nodes = 150000

nodes_per_graph = 5000

graphs = int(nodes/nodes_per_graph)

query0 = 'MATCH (n) DETACH DELETE n'


query1 = Template('CALL apoc.generate.ba( ${nodes_per_graph}, 1, "Cinema", "HAS_LOCATION") '
).substitute(locals())


query2 = '''
MATCH (c:Cinema) SET c.cinemaId = id(c)+1000000
;
'''
query3 = '''
CREATE CONSTRAINT ON (c:Cinema) ASSERT c.cinemaId IS UNIQUE
;
'''
query4 = '''
CREATE CONSTRAINT ON (a:Account) ASSERT a.accountId IS UNIQUE
;
'''

query5 = '''
CREATE INDEX on :DailyBoxOffice(accountId)
;    
'''

query6 = '''
MATCH (c:Cinema)
WHERE NOT EXISTS ( (c)-[:HAS_ACCOUNT]->() )
CREATE (a:Account)<-[:HAS_ACCOUNT]-(c) SET a.accountId = c.cinemaId
;
'''


driver = GraphDatabase.driver("bolt://localhost",
                          auth=basic_auth("neo4j", "neo4j"),
                          encrypted=False,
                          trust=TRUST_ON_FIRST_USE)
try:

    session = driver.session()
    result = session.run(query0)
    summary = result.consume()
    print(summary.counters)
    session.close()

    session = driver.session()
    for i in range(graphs):
        result = session.run(query1)
        summary = result.consume()
        #print(summary.counters)
    session.close()

    session = driver.session()
    result = session.run(query2)
    summary = result.consume()
    print(summary.counters)
    session.close()

    session = driver.session()
    result = session.run(query3)
    summary = result.consume()
    print(summary.counters)
    session.close()

    session = driver.session()
    result = session.run(query4)
    summary = result.consume()
    print(summary.counters)
    session.close()

    session = driver.session()
    result = session.run(query5)
    summary = result.consume()
    print(summary.counters)
    session.close()

    session = driver.session()
    result = session.run(query6)
    summary = result.consume()
    print(summary.counters)
    session.close()


except Exception as e:

    print('*** Got exception',e)
    if not isinstance(e, CypherError):
        print('*** Rolling back')
        session.rollback()
    else:
        print('*** Not rolling back')

finally:        
     print('*** Done!')
```

You should see this output:

```
{}
{'properties_set': 150000}
{'constraints_added': 1}
{'constraints_added': 1}
{'indexes_added': 1}
{'relationships_created': 150000, 'nodes_created': 150000, 'labels_added': 150000, 'properties_set': 150000}
*** Done!

```

## Initialization

Now that we've built the graph, let's update it.  The first task is to generate the messages we'll need (pretending that these have been extracted from various source systems as noted above).

This first script initializes some of the global variables used by both the producer and consumer and also sets the format of our messages.  To keep things simple, we'll just have our message be a comma-delimited string with accountId, a revenue number, and a timestamp.

We are also declaring our Kafka topic name and the total number of messages (150,000). Note that if you re-run the producer it will append messages to the existing topic each time.  If you want a new group of 150,000 messages, you'll need a new topic name.

A timer wrapper is included so you can see the throughput.

```
# Initializations.
import random
import time

# connect to Kafka
bootstrap_servers = 'localhost:9092' # change if your brokers live else where

kafka_topic = 'neo4j-150K-demo'

msg_count = 150000

# this is the total number of messages that will be generated

# function to generate messages that will be the data for the graph update

# an example message is displayed : accountId, revenue, timestamp
# this simulates data from the source database

i=0
def generate_message(i):
    msg_payload = (str(i+1000000) + ',' + str(random.randrange(100000,1000000)/100) + ',' + str(time.time())).encode()
    return(msg_payload)

example_message = generate_message(i)
msg_bytes = len(generate_message(i))

print("Example message: " + str(example_message))
print("Message size (bytes): " + str(msg_bytes))


# we'll use a timer so you can see the throughput for both
# the producer and the consumer

# reset timer for kafka producer and consumer

producer_timings = {}
consumer_timings = {}



# function to calc throughput based on msg count and length

def calculate_thoughput(timing, n_messages=msg_count, msg_size=msg_bytes):
    print("Processed {0} messsages in {1:.2f} seconds".format(n_messages, timing))
    print("{0:.2f} MB/s".format((msg_size * n_messages) / timing / (1024*1024)))
    print("{0:.2f} Msgs/s".format(n_messages / timing))

```

Executing this script yields an example message:

```
Example message: b'1000000,9611.06,1483655125.567612'
Message size (bytes): 33
```

## Kafka Message Producer using Confluent_Kafka Client

This defines the producer, and you can see that all we are really doing here is invoking the generate_message() function in a loop. This creates a bunch of messages that are passed to the Kafka broker.

```
# kafka producer function, simulates ETL data stream for graph updates

from confluent_kafka import Producer, KafkaException, KafkaError
import random
import time


topic = kafka_topic

def confluent_kafka_producer_performance():

    # Note that you need to set producer buffer to at least as large as number of messages
    # otherwise you'll get a buffer overflow and the sequential messages will be corrupted
    conf = {'bootstrap.servers': bootstrap_servers,
            'queue.buffering.max.messages': 200000
    }

    producer = confluent_kafka.Producer(**conf)
    i = 0
    messages_overflow = 0
    producer_start = time.time()
    for i in range(msg_count):
        msg_payload = generate_message(i)
        try:
            producer.produce(topic, value=msg_payload)
        except BufferError as e:
            messages_overflow += 1

    # checking for overflow
    print('BufferErrors: ' + str(messages_overflow))

    producer.flush()

    return time.time() - producer_start

```

## Run the Producer

So now we can run the producer (wrapped in the timer function)

```
producer_timings['confluent_kafka_producer'] = confluent_kafka_producer_performance()
calculate_thoughput(producer_timings['confluent_kafka_producer'])

```

We see 150,000 message produced in 1.5 seconds, almost 100K messages per sec.

Kafka is fast, even on my laptop....

```
BufferErrors: 0
Processed 150000 messsages in 1.56 seconds
3.03 MB/s
96150.94 Msgs/s

```

## Validate Produced Messages by Inspecting Offsets

So what did we get? We can use the pykafka client to easily check the earliest and latest offsets and make sure everything looks good.

```
from pykafka import KafkaClient

client = KafkaClient(hosts=bootstrap_servers)
topic = client.topics[kafka_topic.encode()]
print(topic.earliest_available_offsets())
print(topic.latest_available_offsets())

```

We can see that the messages start at offset 0 and go to offset 150,000.
There are 150,000 messages waiting in queue to be consumed.

```
{0: OffsetPartitionResponse(offset=[0], err=0)}
{0: OffsetPartitionResponse(offset=[150000], err=0)}

```


## Kafka Message Consumer using Confluent_Kafka, with Neo4j BOLT Protocol Connector

Now we are ready to update our Cinema graph.

We'll start as before, by defining the consumer, however we're going to make a few optimizations for Neo4j.

The Confluent-Kafka client consumer.poll() function polls one message at at time, so we could generate a single update for each message, but that's a lot of unnecessary I/O for Bolt.  Alternatively, we could try to jam all 150,000 messages into Neo4j but this could create memory issues and a long-running query.  Instead, we'll poll the messages in configurable batches, and send the batch to Neo4j in a parameterized query.

The `confluent_kafka_consume_batch(consumer, batch_size)` function polls and formats a list of messages for Neo4j, per the specified batch size.

```
import confluent_kafka
from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import getopt
import json
from pprint import pformat
import uuid
from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError
#import pandas as pd  #uncomment if you want to write messages to a file



def confluent_kafka_consume_batch(consumer, batch_size):

            batch_list = []

            batch_msg_consumed = 0

            for m in range(batch_size):

                msg = consumer.poll()

                if msg is None:
                    break
                    #continue

                if msg.error():
                    # Error or event
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        # Error
                        raise KafkaException(msg.error())  

                else:

                    datastr = str(msg.value())
                    data = datastr[2:-1].split(",")

                    # details you can access from message object
                    # print("%s %s" % ("iterator:", m))
                    # print("%s %s" % ("msg:", str(msg.value())))
                    # print("%s %s" % ("length:", len(msg)))
                    # print("%s %s" % ("data:", data))

                    batch_list.extend([data])

                    batch_msg_consumed += 1

            return(batch_list, batch_msg_consumed)

```

The `confluent_kafka_consumer_performance()` function is just a wrapper for the consumer, which iterates through the required number of batches, and with each iteration opens a new Bolt session and transaction, and passes the batch list as a parameter to the update query.  We UNWIND the list as rows, and then use the indexed ids to efficiently MATCH and MERGE for the cartesian updates.  The Bolt transaction is committed, and the result consumed (and in case you were wondering, the UNWIND list  as a passed parameter is a neat trick from Michael Hunger).

Once all the batches are consumed, the consumer is closed and the throughput computed.

```

def confluent_kafka_consumer_performance():

    topic = kafka_topic
    msg_consumed_count = 0
    batch_size = 10000
    batch_list = []
    nodes = 0
    rels = 0

    driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=False,
                              trust=TRUST_ON_FIRST_USE)


    update_query = '''
    WITH  {batch_list} AS batch_list
    UNWIND batch_list AS rows
    WITH rows, toInteger(rows[0]) AS acctid
    MATCH (a:Account {accountId: acctid})
    MERGE (a)-[r:HAS_DAILY_REVENUE]->(n:DailyBoxOffice {accountId: toInteger(rows[0])})
    ON CREATE SET n.revenueUSD = toFloat(rows[1]), n.createdDate = toFloat(rows[2])
    '''

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 60000,
            'enable.auto.commit': 'true',
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }
    }

    consumer = confluent_kafka.Consumer(**conf)

    consumer_start = time.time()

    def print_assignment (consumer, partitions):
        print('Assignment:', partitions)

    # Subscribe to topics
    consumer.subscribe([topic], on_assign=print_assignment)

    # consumer loop
    try:

        while True:

            # Neo4j Graph update loop using Bolt
            try:     

                session = driver.session()

                batch_list, batch_msg_consumed = confluent_kafka_consume_batch(consumer, batch_size)
                msg_consumed_count += batch_msg_consumed

                # if you want to see what your message batches look like
                # df = pd.DataFrame(batch_list)
                # filename='test_' + str(msg_consumed_count) + '.csv'
                # df.to_csv(path_or_buf= filename)

                # using the Bolt implicit transaction
                #result = session.run(update_query, {"batch_list": batch_list})

                # using the Bolt explicit transaction, recommended for writes
                with session.begin_transaction() as tx:
                    result = tx.run(update_query, {"batch_list": batch_list})
                    tx.success = True;

                    summary = result.consume()
                    nodes = summary.counters.nodes_created
                    rels = summary.counters.relationships_created

                    print("%s %s %s %s" % ("Messages consumed:", msg_consumed_count , "Batch size:", len(batch_list)), end=" ")
                    print("%s %s %s %s" % ("Nodes created:", nodes, "Rels created:", rels))

                if msg_consumed_count >= msg_count:
                    break

            except Exception as e:

                print('*** Got exception',e)
                if not isinstance(e, CypherError):
                    print('*** Rolling back')
                    session.rollback()
                else:
                    print('*** Not rolling back')

            finally:        
                session.close()
                batch_msg_consumed_count = 0


    except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

    finally:
        consumer_timing = time.time() - consumer_start
        consumer.close()    
        return consumer_timing
```

## Run the Consumer, Update Neo4j

Executing the consumer script updates the graph:

```
# run consumer throughput test

consumer_timings['confluent_kafka_consumer'] = confluent_kafka_consumer_performance()

calculate_thoughput(consumer_timings['confluent_kafka_consumer'])

```

The output shows each batch being processed and returns the Neo4j Bolt driver summary showing the nodes and relationships created for each batch.

On my laptop I can process 150,000 updates in 16 secs, at about 9000 messages per sec.

```
Assignment: [TopicPartition{topic=neo4j-150K-demo2,partition=0,offset=-1001,error=None}]
Messages consumed: 10000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 20000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 30000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 40000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 50000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 60000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 70000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 80000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 90000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 100000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 110000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 120000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 130000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 140000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Messages consumed: 150000 Batch size: 10000 Nodes created: 10000 Rels created: 10000
Processed 150000 messsages in 16.80 seconds
0.28 MB/s
8925.97 Msgs/s
```
