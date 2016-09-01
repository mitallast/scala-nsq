package org.mitallast.nsq.netty

import java.nio.charset.Charset
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.typesafe.config.ConfigFactory
import org.mitallast.nsq._
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._

class NSQClientSpec extends FlatSpec with Matchers {

  "nsq producer" should "connect and disconnect correctly" in {
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val producer = client.producer()
    producer.connect("127.0.0.1", 4150)
    producer.close()
    client.close()
  }

  it should "send pub command" in {
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val producer = client.producer()
    producer.connect("127.0.0.1", 4150)
    1 to 10 foreach { _ ⇒
      val future = producer.pub("scala.nsq.test", "hello world".getBytes(Charset.forName("UTF-8")))
      Await.result(future, 10.seconds)
    }
    producer.close()
    client.close()
  }

  it should "send mpub command" in {
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val producer = client.producer()
    producer.connect("127.0.0.1", 4150)
    val future = producer.mpub("scala.nsq.test", Array(
      "hello world".getBytes(Charset.forName("UTF-8")),
      "hello world".getBytes(Charset.forName("UTF-8"))
    ))
    Await.result(future, 10.seconds)
    producer.close()
    client.close()
  }

  "nsq consumer" should "connect and disconnect correctly" in {
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ {
    })
    consumer.connect("127.0.0.1", 4150)
    consumer.close()
    client.close()
  }

  it should "send rdy command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.connect("127.0.0.1", 4150)
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    new String(message.data) should equal("hello world")
    consumer.close()
    client.close()
  }

  it should "send fin command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.connect("127.0.0.1", 4150)
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.fin()
    consumer.close()
    client.close()
  }

  it should "send req command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.connect("127.0.0.1", 4150)
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.req(10)
    consumer.close()
    client.close()
  }

  it should "send touch command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = new NSQNettyClient(ConfigFactory.load("scala-nsq"))
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.connect("127.0.0.1", 4150)
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.touch()
    consumer.close()
    client.close()
  }
}
