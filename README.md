# Scala NSQ client

Scala NSQ client, based on netty, typesafe config, slf4j and json4s.

## Configuration

Configuration based on [typesafe config](https://github.com/typesafehub/config) library.
See [reference](https://github.com/mitallast/scala-nsq/blob/master/src/main/resources/reference.conf) 
for available configuration options

## Create client instance

By default, client connect to `nsqlookupd` node at `http://127.0.0.1:4161`, you can override 
default settings using standard `typesafehub/config` ways.

For consumer instance, client send `GET /lookup?topic=...` for concrete topic, and
send `GET /nodes` for provider instance to retrieve list of addresses `nsqd` nodes and connect to them.

and request `GET /lookup?topic=...`
for concrete topic, o

```scala
import org.mitallast.nsq._

val client = NSQClient()
```



Also, you can provide it programmatically:

```scala
import org.mitallast.nsq._
import com.typesafe.config.Config

val config: Config = ...
val client = NSQClient(config)
```

## Producer API

```scala
val producer = client.producer()

val listener = match {
  case Success(_:OK) => log.info("message pub successfully")
  case Failure(error) => log.info("message pub failed", error)
}

// publish one message 
producer.pub(topic="test", data=Array[Byte](1,0,1,1)).onComplete(listener)
producer.pubStr(topic="test", data="hello").onComplete(listener)

// publish multiple messages
producer.mpub(topic="test", data=Seq(Array[Byte](1,0,1,1), Array[Byte](1,0,1,1))).onComplete(listener)
producer.mpubStr(topic="test", data=Seq("hello", "world")).onComplete(listener)
```


## Consumer API

```scala
val consumer = client.consumer(topic="test", channel="default", consumer= message => {
    log.info("received: {}", msg)
    // send `TOUCH msgid` message request 
    msg.touch() 
    // send `REQ msdid 100` message request
    msg.req(100)
    // send `FIN msgid` message request
    msg.fin()
})

// send "RDY 1" to random node
consumer.ready(1) 

// send "RDY 1" to all nodes
consumer.readyAll(1)
```
