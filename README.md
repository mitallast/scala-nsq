# Scala NSQ client

Scala NSQ client, based on netty, typesafe config, slf4j and json4s.

## Dependency management

Client requires scala 2.11.

For SBT users:

```scala
libraryDependencies += "com.github.mitallast" %% "scala-nsq" % "1.5"
```

For Maven users:

```scala
<dependency>
  <groupId>com.github.mitallast</groupId>
  <artifactId>scala-nsq_2.11</artifactId>
  <version>1.5</version>
</dependency>
```


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

By default, client use `NSQLookupDefault` with configurable addresses
to `nsqlookupd` instances. By default, `http://127.0.0.1:4161` address using.
Set config property `nsq.lookup-address` and `lookup-period` to override.

Also, you can implement trait `NSQLookup` and provide it programmatically:

```scala
import org.mitallast.nsq._
import java.net.InetSocketAddress

val lookup = new NSQLookup {
    def nodes(): List[SocketAddress] = ...
    def lookup(topic: String): List[SocketAddress] = ...
}

val client = NSQClient(lookup)

// or with config:
val config: Config = ...
val client = NSQClient(lookup, config)
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

Consumer automatically send `RDY <number>\n` command. By default, 1 message.
Set config property `nsq.max-ready-count` to override. 


```scala
val consumer = client.consumer(topic="test", channel="default") { message =>
    log.info("received: {}", msg)
    // send `TOUCH msgid` message request 
    msg.touch() 
    // send `REQ msdid 100` message request
    msg.req(100)
    // send `FIN msgid` message request
    msg.fin()
}
```
