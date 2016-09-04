package org.mitallast.nsq

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.mitallast.nsq.protocol.{NSQProtocol, OK}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

class NSQClientSpec extends FlatSpec with Matchers {

  import NSQLocalClient._
  import NSQProtocol._

  "nsq client producer" should "init connection" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    server.handle shouldEqual buf("  V2")
    server.send()

    server.handle shouldEqual requestBuf("IDENTIFY\n", json)
    server.send(responseBuf("OK"))

    producer.close()
    client.close()
    server.close()
  }

  it should "send pub command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))

    val future = producer.pubStr("test", "hello")

    server.handle shouldEqual requestBuf("PUB test\n", "hello")
    server.send(responseBuf("OK"))

    Await.ready(future, 10.seconds)
    future.isCompleted shouldBe true
    future.value.get shouldEqual Success(OK())

    producer.close()
    client.close()
    server.close()
  }

  it should "send mpub command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))

    val future = producer.mpubStr("scala.nsq.test", Array("hello", "world"))

    server.handle shouldEqual requestBuf("MPUB scala.nsq.test\n", buf(buf(2), buf(5), buf("hello"), buf(5), buf("world")))
    server.send(responseBuf("OK"))

    Await.ready(future, 10.seconds)
    future.isCompleted shouldBe true
    future.value.get shouldEqual Success(OK())

    producer.close()
    client.close()
    server.close()
  }

  "nsq consumer" should "init connection" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ {
    })

    server.handle shouldEqual buf("  V2")
    server.send()

    server.handle shouldEqual requestBuf("IDENTIFY\n", json)
    server.send(responseBuf("OK"))

    server.handle shouldEqual buf("SUB scala.nsq.test default\n")
    server.send()

    consumer.close()
    client.close()
    server.close()
  }

  it should "send rdy command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()

    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer("scala.nsq.test", consumer = queue.offer(_))

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))
    server.handle
    server.send()

    consumer.ready(1)

    server.handle shouldEqual buf("RDY 1\n")
    server.send(messageBuf("hello"))

    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    new String(message.data) shouldEqual "hello"

    consumer.close()
    client.close()
    server.close()
  }

  it should "send fin command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()

    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer("scala.nsq.test", consumer = queue.offer(_))

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))
    server.handle
    server.send()

    consumer.ready(1)

    server.handle
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).fin()

    server.handle shouldEqual buf(s"FIN $messageId\n")
    server.send()

    consumer.close()
    client.close()
    server.close()
  }

  it should "send req command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()

    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer("scala.nsq.test", consumer = queue.offer(_))

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))
    server.handle
    server.send()

    consumer.ready(1)

    server.handle
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).req(100)

    server.handle shouldEqual buf(s"REQ $messageId 100\n")
    server.send()

    consumer.close()
    client.close()
    server.close()
  }

  it should "send touch command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()

    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer("scala.nsq.test", consumer = queue.offer(_))

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))
    server.handle
    server.send()

    consumer.ready(1)

    server.handle
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).touch()

    server.handle shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    consumer.close()
    client.close()
    server.close()
  }

  it should "schedule send touch command" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()

    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer("scala.nsq.test", consumer = queue.offer(_))

    server.handle
    server.send()
    server.handle
    server.send(responseBuf("OK"))
    server.handle
    server.send()

    consumer.ready(1)

    server.handle
    server.send(messageBuf("hello"))

    val message = queue.poll(10, TimeUnit.SECONDS)
    message.touch(100.millis)

    server.handle shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    server.handle shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    server.handle shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    message.fin()

    server.handle
    server.send()

    consumer.close()
    client.close()
    server.close()
  }
}
