package org.mitallast.nsq

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.typesafe.config.ConfigFactory
import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.local.{LocalAddress, LocalChannel, LocalEventLoopGroup, LocalServerChannel}
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.util.CharsetUtil
import org.mitallast.nsq.protocol.{NSQConfig, OK}
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Success

object NSQClientSpec {
  val log = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load("scala-nsq")
  val localAddr = new LocalAddress("nsq.id")
  val json ="""{"client_id":"test","hostname":"localhost","feature_negotiation":true,"user_agent":"test"}"""
  val timestamp = System.currentTimeMillis()
  val attempts = 2
  val messageId = "WCKHEOWCMPWECHWQ"

  def buf(buffers: ByteBuf*) = Unpooled.wrappedBuffer(buffers: _*)

  def buf(value: Int) = Unpooled.buffer(4).writeInt(value)

  def buf(value: Long) = Unpooled.buffer(8).writeLong(value)

  def buf(value: String) = Unpooled.copiedBuffer(value, CharsetUtil.UTF_8)

  def buf(value: Array[Byte]) = Unpooled.wrappedBuffer(value)

  def requestBuf(header: String, data: ByteBuf): ByteBuf = buf(buf(header), buf(data.readableBytes()), data)

  def requestBuf(header: String, data: String): ByteBuf = requestBuf(header, buf(data))

  def responseBuf(data: String) = {
    val b = buf(data)
    buf(buf(b.readableBytes() + 4), buf(0), b)
  }

  def messageBuf(data: String): ByteBuf = messageBuf(buf(data))

  def messageBuf(data: ByteBuf): ByteBuf = Unpooled.buffer()
    .writeInt(4 + 8 + 2 + 16 + data.readableBytes())
    .writeInt(2)
    .writeLong(timestamp)
    .writeShort(attempts)
    .writeBytes(messageId.getBytes(CharsetUtil.US_ASCII))
    .writeBytes(data)

  case class LocalNSQNettyClient() extends NSQNettyClient(config) {

    override private[nsq] def newBootstrap: Bootstrap = new Bootstrap()
      .channel(classOf[LocalChannel])
      .group(new LocalEventLoopGroup())

    override private[nsq] val nsqConfig: NSQConfig = NSQConfig("test", "localhost", "test")

    override private[nsq] val lookup: NSQLookup = new NSQLookup(List.empty) {

      override def nodes() = List(localAddr)

      override def lookup(topic: String) = List(localAddr)
    }

    override def close(): Unit = bootstrap.group().shutdownGracefully(0, 0, TimeUnit.MICROSECONDS)
  }

  case class LocalNSQNettyServer() {

    val request = new LinkedBlockingQueue[ByteBuf]()
    val response = new LinkedBlockingQueue[Option[ByteBuf]]()

    val bootstrap = new ServerBootstrap()
      .channel(classOf[LocalServerChannel])
      .group(new LocalEventLoopGroup())
      .childHandler(new ChannelInitializer[LocalChannel] {
        override def initChannel(ch: LocalChannel) = {
          ch.pipeline().addLast(new SimpleChannelInboundHandler[ByteBuf] {
            override def channelRead0(ctx: ChannelHandlerContext, msg: ByteBuf) = {
              log.info("message received: {}", msg.readableBytes())
              request.offer(msg)
              response.poll(1, MINUTES).foreach(ctx.writeAndFlush(_))
            }
          })
        }
      })

    val channel = bootstrap.bind(localAddr).sync().channel()

    def close() = {
      channel.close().sync()
    }

    def handle = request.poll(1, MINUTES)

    def send(buf: ByteBuf) = response.offer(Some(buf))

    def send() = response.offer(None)
  }
}

class NSQClientSpec extends FlatSpec with Matchers {

  import NSQClientSpec._

  "nsq client producer" should "init connection" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    server.handle shouldEqual buf("  V2")
    server.send()

    server.handle shouldEqual requestBuf("IDENTIFY\n", json)
    server.send(responseBuf("{}"))

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
    server.send(responseBuf("{}"))

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
    server.send(responseBuf("{}"))

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
    server.send(responseBuf("{}"))

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
    server.send(responseBuf("{}"))
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
    server.send(responseBuf("{}"))
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
    server.send(responseBuf("{}"))
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
    server.send(responseBuf("{}"))
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
}
