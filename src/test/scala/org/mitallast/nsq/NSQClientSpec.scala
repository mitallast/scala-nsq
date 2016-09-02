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

class NSQClientSpec extends FlatSpec with Matchers {

  val log = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load("scala-nsq")
  val localAddr = new LocalAddress("nsq.id")
  val json ="""{"client_id":"test","hostname":"localhost","feature_negotiation":true,"user_agent":"test"}""".getBytes

  def buf(buffers: ByteBuf*) = Unpooled.wrappedBuffer(buffers: _*)

  def buf(value: Int) = Unpooled.buffer(4).writeInt(value)

  def buf(value: String) = Unpooled.copiedBuffer(value, CharsetUtil.UTF_8)

  def buf(value: Array[Byte]) = Unpooled.wrappedBuffer(value)

  def identityBuf = buf(buf("IDENTIFY\n"), buf(json.length), buf(json))

  case class LocalNSQNettyClient() extends NSQNettyClient(config) {

    override private[nsq] def newBootstrap: Bootstrap = new Bootstrap()
      .channel(classOf[LocalChannel])
      .group(new LocalEventLoopGroup())

    override private[nsq] val nsqConfig: NSQConfig = NSQConfig("test", "localhost", "test")

    override private[nsq] val lookup: NSQLookup = new NSQLookup(List.empty) {

      override def nodes() = List(localAddr)

      override def lookup(topic: String) = List(localAddr)
    }
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

  "nsq client producer" should "init connection" in {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    server.handle shouldEqual buf("  V2")
    server.send()
    server.handle shouldEqual identityBuf
    server.send(buf(buf(6), buf(0), buf("{}")))

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
    server.send(buf(buf(6), buf(0), buf("{}")))

    val future = producer.pubStr("test", "hello")

    server.handle shouldEqual buf(buf("PUB test\n"), buf(5), buf("hello"))
    server.send(buf(buf(6), buf(0), buf("OK")))

    Await.ready(future, 10.seconds)
    future.isCompleted shouldBe true
    future.value.get shouldEqual Success(OK())

    producer.close()
    client.close()
    server.close()
  }

  it should "send mpub command" in {
    val client = NSQClient()
    val producer = client.producer()
    val future = producer.mpubStr("scala.nsq.test", Array("hello world", "hello world"))
    Await.result(future, 10.seconds)
    producer.close()
    client.close()
  }

  "nsq consumer" should "connect and disconnect correctly" in {
    val client = NSQClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ {
    })
    consumer.close()
    client.close()
  }

  it should "send rdy command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = NSQClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    new String(message.data) should equal("hello world")
    consumer.close()
    client.close()
  }

  it should "send fin command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = NSQClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.fin()
    consumer.close()
    client.close()
  }

  it should "send req command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = NSQClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.req(10)
    consumer.close()
    client.close()
  }

  it should "send touch command" in {
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val client = NSQClient()
    val consumer = client.consumer("scala.nsq.test", consumer = message ⇒ queue.offer(message))
    consumer.ready(1)
    val message = queue.poll(10, TimeUnit.SECONDS)
    message should not be null
    message.touch()
    consumer.close()
    client.close()
  }
}
