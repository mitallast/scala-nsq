package com.github.mitallast.nsq

import java.util.concurrent.TimeUnit

import com.github.mitallast.nsq.protocol.{NSQProtocol, OKFrame}
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

class NSQClientSpec extends FlatSpec with Matchers {

  import NSQLocalClient._
  import NSQProtocol._

  "nsq client" should "fallback to reference conf" in {
    val client = NSQClient(ConfigFactory.parseString("{}"))
    client.close()
  }

  "nsq client producer" should "init connection" in producer { (server, client, producer) ⇒
    server.handle() shouldEqual buf("  V2")
    server.send()

    server.handle() shouldEqual requestBuf("IDENTIFY\n", json)
    server.send(responseBuf("OK"))
  }

  it should "send pub command" in producer { (server, client, producer) ⇒
    server.initialize()

    val future = producer.pubStr("test", "hello")

    server.handle() shouldEqual requestBuf("PUB test\n", "hello")
    server.send(responseBuf("OK"))

    Await.ready(future, 10.seconds)
    future.isCompleted shouldBe true
    future.value.get shouldEqual Success(OKFrame)
  }

  it should "send mpub command" in producer { (server, client, producer) ⇒
    server.initialize()

    val future = producer.mpubStr("scala.nsq.test", Array("hello", "world"))

    server.handle() shouldEqual requestBuf("MPUB scala.nsq.test\n", buf(buf(2), buf(5), buf("hello"), buf(5), buf("world")))
    server.send(responseBuf("OK"))

    Await.ready(future, 10.seconds)
    future.isCompleted shouldBe true
    future.value.get shouldEqual Success(OKFrame)
  }

  it should "handle E_INVALID error" in producer { (server, client, producer) ⇒
    server.initialize()
    val future = producer.pubStr("test", "message")
    server.handle()
    server.send(errorBuf(NSQError.E_INVALID))
    an[NSQErrorInvalid] should be thrownBy Await.result(future, 10.seconds)
  }

  it should "cancel futures on close server channel" in producer { (server, client, producer) ⇒
    server.initialize()
    val future = producer.pubStr("test", "message")
    server.skip().close()
    an[NSQDisconnected] should be thrownBy Await.result(future, 10.seconds)
  }

  it should "cancel futures on close client channel" in producer { (server, client, producer) ⇒
    server.initialize()
    val future = producer.pubStr("test", "message")
    server.skip()
    producer.close()
    an[NSQDisconnected] should be thrownBy Await.result(future, 10.seconds)
  }

  it should "cancel future on non matched error type" in producer { (server, client, producer) ⇒
    server.initialize()
    val future = producer.pubStr("test", "message")
    server.handle()
    server.send(errorBuf(NSQError.E_FIN_FAILED))
    an[NSQErrorFinFailed] should be thrownBy Await.result(future, 10.seconds)
  }

  "nsq client consumer" should "init connection" in consumer { (server, client, queue, consumer) ⇒
    server.handle shouldEqual buf("  V2")
    server.send()

    server.handle shouldEqual requestBuf("IDENTIFY\n", json)
    server.send(responseBuf("OK"))

    server.handle shouldEqual buf("SUB scala.nsq.test default\n")
    server.send(responseBuf("OK"))

    server.handle shouldEqual buf("RDY 1\n")
    server.send()
  }

  it should "send rdy command" in consumer { (server, client, queue, consumer) ⇒
    server.initialize().ok()

    server.handle() shouldEqual buf("RDY 1\n")
    server.send(messageBuf("hello"))

    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null

    new String(message.data) shouldEqual "hello"
  }

  it should "send fin command" in consumer { (server, client, queue, consumer) ⇒
    server.initialize().ok()

    server.handle()
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).fin()

    server.handle() shouldEqual buf(s"FIN $messageId\n")
    server.send()
  }

  it should "send req command" in consumer { (server, client, queue, consumer) ⇒
    server.initialize().ok()

    server.handle()
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).req(100.milliseconds)

    server.handle shouldEqual buf(s"REQ $messageId 100\n")
    server.send()
  }

  it should "send touch command" in consumer { (server, client, queue, consumer) ⇒
    server.initialize().ok()

    server.handle()
    server.send(messageBuf("hello"))

    queue.poll(10, TimeUnit.SECONDS).touch()

    server.handle() shouldEqual buf(s"TOUCH $messageId\n")
    server.send()
  }

  it should "schedule send touch command" in consumer { (server, client, queue, consumer) ⇒
    server.initialize().ok()

    server.handle()
    server.send(messageBuf("hello"))

    val message = queue.poll(10, TimeUnit.SECONDS)
    message.touch(100.millis)

    server.handle() shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    server.handle() shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    server.handle() shouldEqual buf(s"TOUCH $messageId\n")
    server.send()

    message.fin()

    server.skip()
  }
}
