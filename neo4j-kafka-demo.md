##Neo4j GraphGist - Enterprise Architectures: Real-time Graph Updates using Kafka Messaging

# Neo4j Use Case: Low Latency Graph Analytics & OLTP - Update 1M Nodes in 90 secs with Kafka and Neo4j Bolt

## Introduction

A recent [Neo4j whitepaper](https://neo4j.com/blog/oracle-rdbms-neo4j-fully-sync-data/) describes how Monsanto is performing real-time updates on a 600M node Neo4j graph using Kafka to consume data extracted from a large Oracle Exadata instance.

This modern data architecture combines a fast, scalable messaging platform (Kafka) for low latency data provisioning and an enterprise graph database (Neo4j) for high performance, in-memory analytics & OLTP - creating new and powerful real-time graph analytics capabilities for your enterprise applications.

In this Gist, we'll see how Apache Kafka can stream 1M messages to update a large Neo4j graph.


### Kafka Distributed Streaming Platform

[Kakfa](https://kafka.apache.org/) allows you to create a messaging service that is fault-tolerant, scalable, and features massively parallel distributed processing.

Kafka was originally developed at LinkedIn, and is becoming widely adopted because it excels at moving large amounts of data quickly across the enterprise.

[Using Kafka](https://techbeacon.com/what-apache-kafka-why-it-so-popular-should-you-use-it), LinkedIn has ingested over a trillion messages per day, while Netflix reports ingesting over 500B messages per day on AWS.

<img width="888" alt="netflix" src =
"https://cloud.githubusercontent.com/assets/5991751/21917264/806aebe4-d8fb-11e6-8c95-99105a429b7f.png">

The core of Kafka is the message log, which is essentially a time-dependent data table.  Messages are identified only by their offset in the log, and the log represents a running record of events published by source systems. Applications can subscribe to message streams, allowing for loosely coupled, flexible architectures.

In a production system, the Kafka producers and consumers would be running continuously, with the producers polling for changes in the source data, and the consumers polling to see if there are new messages in the Kafka log for updates.

There are many common scenarios where the lowest possible latency between source systems and Neo4j is desirable (e.g. recommendations, e-commerce, fraud detection, access control, supply chain).  Kafka + Neo4j is a great fit for these use cases.


### Neo4j Kafka Demo

Let's imagine we're operating a service called MovieFriends that features a social network of 1M users who rent streaming movies.   Our goal is to ingest an update of daily charges for each user.  We have a Neo4j social graph of User nodes, and we want to append a DailyCharge node to each User node.

<img width="888" alt="moviefriends" src="https://cloud.githubusercontent.com/assets/5991751/21914059/86fc04fe-d8e5-11e6-8bdf-60aa6be1b7de.png">

We'll setup Kafka & Neo4j, create a 1M node Neo4j graph, produce a 1M messages in just a few seconds, and then consume these messages as batch updates to our graph.

  * We'll set up Kafka and Neo4j instances on your local machine (about an hour)
  * Next, we'll use a simple producer client to publish messages to Kafka, mocking an extract from a source system (5 secs)
  * Finally, we'll use a simple consumer to subscribe to Kakfa and send 1M batched messages as updates to Neo4j using the high-speed Bolt protocol (90 secs)

The confluent-kafka client is currently the fastest python client available, per [recent benchmarking](http://activisiongamescience.github.io/2016/06/15/Kafka-Client-Benchmarking/) by the Activision data sciences team.

I've used the Activision benchmarking script as the framework for this demo -- the main modifications I've made are to generate more realistic messages in the Kafak producer, and integrate the Bolt driver with the Kafka consumer.


---


## PART 1: Setting up Kafka and Neo4j

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


### Neo4j Kafka Demo Dependencies


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

There are three dependencies [confluent-python](https://github.com/confluentinc/confluent-kafka-python), [pykafka](https://github.com/Parsely/pykafka), and [neo4j-driver](https://github.com/neo4j/neo4j-python-driver).

You'll need to be running Python 3.5 and Jupyter if you want to run the notebook.

If you are new to Python, check out the Anaconda distribution.

```
$ pip install confluent-kafka
$ pip install pykakfa
$ pip install neo4j-driver

```

*Note on confluent-kafka: This has a dependency on librdkafka, and will complain if it can't find it. You can install librdkafka into your python home from source, using the configure command.  
(I use Anaconda python, so I put it there)*

```
librdkafka-master $ ./configure --prefix=/Users/michael/anaconda
librdkafka-master $ make -j
librdkafka-master $ sudo make install
librdkafka-master $ pip install confluent-kafka

```


### Step 3. Install Neo4j & APOC dependencies

I'm using Neo4j 3.1 released in Dec 2016.  If you haven't seen this latest version, it's really worth checking out, lots of new enterprise features.  APOC is a huge collection of really useful Neo4j utilities, we'll use some here.

You can get Neo4j here, get the Enterprise versions (so you can use plugins).

https://neo4j.com/download/

Open a new terminal window and start Neo4j. You'll be asked to change your password if this is a new install.

```
$ ./bin/neo4j start
Starting Neo4j.
Started neo4j (pid 69178). By default, it is available at http://localhost:7474/
There may be a short delay until the server is ready.

```
For this demo I'm using the default "neo4j" password.
You can put in a dummy password "test" and then set it back to the default password "neo4j".
Use this command in the Neo4j browser to get the password prompts:

```
:server change-password
```

Now shutdown Neo4j and install the APOC package .jar file in the neo4j /plugins folder.

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


---


## PART 2:  Creating the Neo4j Graph Database and Kafka Messages

Let's start by making the MovieFriends graph. You'll need to start with a new, blank Neo4j database.

To achieve good performance, we'll set constraints and indexes on all id fields we'll use for matching update records.

```
#make sure apoc procedures are installed in Neo4j plugins folder

from neo4j.v1 import GraphDatabase, basic_auth, TRUST_ON_FIRST_USE, CypherError
from string import Template


nodes = 1000000

nodes_per_graph = 10000

graphs = int(nodes/nodes_per_graph)

query0 = 'MATCH (n) DETACH DELETE n'


query1 = Template('CALL apoc.generate.ba( ${nodes_per_graph}, 1, "User", "KNOWS") '
).substitute(locals())


query2 = '''
MATCH (n:User) SET n.userId = id(n)+1000000
;
'''
query3 = '''
CREATE CONSTRAINT ON (n:User) ASSERT n.userId IS UNIQUE
;
'''

query4 = '''
CREATE INDEX on :DailyCharge(userId)
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
{'properties_set': 1000000}
{'constraints_added': 1}
{'indexes_added': 1}
*** Done!

```


### Initialization

Now that we've built the graph, let's update it.  The first task is to generate the messages we'll need (pretending that these have been extracted from various source systems as noted above).

This first script initializes some of the global variables used by both the producer and consumer and also sets the format of our messages.  To keep things simple, we'll just have our message be a comma-delimited string with a userId, an amount, and a timestamp.

We are also declaring our Kafka topic name and the total number of messages. Note that if you re-run the producer it will append messages to the existing topic each time.  If you want a new group of 1M messages, you'll need a new topic name.

A timer wrapper is included so you can see the throughput.

```
# Initializations.
import random
import time

# connect to Kafka
bootstrap_servers = 'localhost:9092' # change if your brokers live else where

kafka_topic = 'neo4j-1M-demo'

msg_count = 1000000

# this is the total number of messages that will be generated

# function to generate messages that will be the data for the graph update

# an example message is displayed : userId, amount, timestamp
# this simulates data from the source database

i=0
def generate_message(i):
    msg_payload = (str(i+1000000) + ',' + str(random.randrange(0,5000)/100) + ',' + str(time.time())).encode()
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
Example message: b'1000000,5.84,1484261697.689981'
Message size (bytes): 30

```


### Kafka Message Producer using Confluent_Kafka Client

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


### Run the Producer

So now we can run the producer (wrapped in the timer function)

```
producer_timings['confluent_kafka_producer'] = confluent_kafka_producer_performance()
calculate_thoughput(producer_timings['confluent_kafka_producer'])

```

We see 1M message produced in 5 seconds, about 200K messages per sec (on my laptop).

```
BufferErrors: 0
Processed 1000000 messsages in 5.06 seconds
5.66 MB/s
197728.85 Msgs/s

```


### Validate Produced Messages by Inspecting Offsets

So what did we get? We can use the pykafka client to easily check the earliest and latest offsets and make sure everything looks good.

```
from pykafka import KafkaClient

client = KafkaClient(hosts=bootstrap_servers)
topic = client.topics[kafka_topic.encode()]
print(topic.earliest_available_offsets())
print(topic.latest_available_offsets())

```

We can see that the messages start at offset 0 and go to offset 1,000,000.
There are 1M messages waiting in queue to be consumed.

```
{0: OffsetPartitionResponse(offset=[0], err=0)}
{0: OffsetPartitionResponse(offset=[1000000], err=0)}

```
---


## PART 3. Consuming Kafka Messages and Updating Neo4j


### Confluent Consumer

Now we are ready to update our MovieFriends graph.

We'll start as before, by defining the consumer, however we're going to make a few optimizations for Neo4j.

The Confluent-Kafka client consumer.poll() function polls one message at at time, so we could generate a single update for each message, but that's a lot of unnecessary I/O for Bolt.  Alternatively, we could try to jam all 1M messages into Neo4j but this could create memory issues and a long-running query. A better approach is to poll the messages in batches, and then send each batch to Neo4j as a list in a parameterized query.

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


### Neo4j Bolt Update


The `confluent_kafka_consumer_performance()` function is a wrapper for the consumer, which iterates through the required number of batches.  With each iteration, it opens a new Bolt transaction, and passes the batch list as a parameter to the update query.  We UNWIND the list as rows, and then use the indexed ids to efficiently MATCH and MERGE for the cartesian updates.  The Bolt transaction is committed, and the result consumed (and in case you were wondering, the UNWIND list  as a passed parameter is a neat trick from Michael Hunger).

Once all the batches are consumed, the consumer is closed and the throughput computed.

```
def confluent_kafka_consumer_performance():

    topic = kafka_topic
    msg_consumed_count = 0
    batch_size = 50000
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
    WITH rows, toInteger(rows[0]) AS userid
    MATCH (u:User {userId: userid})
    MERGE (u)-[r:HAS_DAILY_CHARGE]->(n:DailyCharge {userId: toInteger(rows[0])})
    ON CREATE SET n.amountUSD = toFloat(rows[1]), n.createdDate = toFloat(rows[2])
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

        session = driver.session()

        while True:

            # Neo4j Graph update loop using Bolt
            try:     

                batch_list, batch_msg_consumed = confluent_kafka_consume_batch(consumer, batch_size)
                msg_consumed_count += batch_msg_consumed

                # if you want to see what your message batches look like
                # df = pd.DataFrame(batch_list)
                # filename='test_' + str(msg_consumed_count) + '.csv'
                # df.to_csv(path_or_buf= filename)

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
                batch_msg_consumed_count = 0


    except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

    finally:
        session.close()
        consumer_timing = time.time() - consumer_start
        consumer.close()    
        return consumer_timing

```


### Run the Consumer

Executing the consumer script updates the graph:

```
# run consumer throughput test

consumer_timings['confluent_kafka_consumer'] = confluent_kafka_consumer_performance()

calculate_thoughput(consumer_timings['confluent_kafka_consumer'])

```

The output shows each batch being processed and returns the Neo4j Bolt driver summary showing the nodes and relationships created for each batch.

I can process 1M updates in 90 secs on my laptop, about 11,000 messages per sec.  

You can test different batch sizes and see how that affects performance.

*Note that in this demo, I'm running all the transactions in the same session to maximize throughput, in production it may safer to open and close a new session with each batch transaction (ie inside the update loop).*

```
Assignment: [TopicPartition{topic=neo4j-1M-demo,partition=0,offset=-1001,error=None}]
Messages consumed: 50000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 100000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 150000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 200000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 250000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 300000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 350000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 400000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 450000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 500000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 550000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 600000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 650000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 700000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 750000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 800000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 850000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 900000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 950000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Messages consumed: 1000000 Batch size: 50000 Nodes created: 50000 Rels created: 50000
Processed 1000000 messsages in 91.31 seconds
0.31 MB/s
10951.72 Msgs/s
```


## Summary

In this GraphGist, I provided a simple demonstration of how Kafka can be integrated with Neo4j to create a high-throughput, loosely-coupled ETL using Kafka's simple consumer and Neo4j's high-speed Bolt protocol. Neo4j has excellent OLTP capabilities and when coupled with the Kakfa distributed streaming platform, you now have the opportunity to build highly-scalable, real-time graph analytics into your enterprise applications.  

Special thanks to Tim Williamson (Monsanto), Michael Hunger (Neo4j), Michael Kilgore (InfoClear).

For more information about how Neo4j can supercharge your enterprise analytics, contact:
>
>Michael Moore
>
>Graph Architect & Certified Neo4j Professional
>
>michael.moore@graphadvantage.com
>
>http://www.graphadvantage.com
