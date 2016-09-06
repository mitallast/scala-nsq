package com.github.mitallast.nsq

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import io.netty.bootstrap.{Bootstrap, ServerBootstrap}
import io.netty.buffer.ByteBuf
import io.netty.channel.{ChannelHandlerContext, ChannelInitializer, SimpleChannelInboundHandler}
import io.netty.channel.local.{LocalChannel, LocalEventLoopGroup, LocalServerChannel}
import com.github.mitallast.nsq.protocol.NSQConfig
import com.github.mitallast.nsq.protocol.NSQProtocol

import scala.concurrent.duration._

object NSQLocalClient {

  import NSQProtocol._

  case class LocalNSQNettyClient() extends NSQNettyClient(config) {

    override private[nsq] def newBootstrap: Bootstrap = new Bootstrap()
      .channel(classOf[LocalChannel])
      .group(new LocalEventLoopGroup())

    override private[nsq] val nsqConfig: NSQConfig = NSQConfig("test", "localhost", "test")

    override private[nsq] val lookup: NSQLookup = new NSQLookup(List.empty) {

      override def nodes() = List(localAddr)

      override def lookup(topic: String) = List(localAddr)
    }

    override def close(): Unit = bootstrap.group().shutdownGracefully(10, 10, TimeUnit.MILLISECONDS).sync()
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
              response.poll(1, MINUTES).foreach(msg ⇒ {
                log.info("response: {}", msg.readableBytes())
                ctx.writeAndFlush(msg)
              })
            }
          })
        }
      })

    val channel = bootstrap.bind(localAddr).sync().channel()

    def close() = {
      if(channel.isOpen){
        channel.close().sync()
      }
      bootstrap.group().shutdownGracefully(10, 10, TimeUnit.MILLISECONDS).sync()
    }

    def handle = request.poll(1, MINUTES)

    def initialize() = skip().ok()

    def skip() = {
      handle
      send()
      this
    }

    def ok() = {
      handle
      send(responseBuf("OK"))
      this
    }

    def send(buf: ByteBuf) = response.offer(Some(buf))

    def send() = response.offer(None)
  }

  case class producer(fun: (LocalNSQNettyServer, LocalNSQNettyClient, NSQProducer) ⇒ Unit) {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val producer = client.producer()

    try {
      fun(server, client, producer)
    } finally {
      producer.close()
      client.close()
      server.close()
    }
  }

  case class consumer(fun: (LocalNSQNettyServer, LocalNSQNettyClient, LinkedBlockingQueue[NSQMessage], NSQConsumer) ⇒ Unit) {
    val server = LocalNSQNettyServer()
    val client = LocalNSQNettyClient()
    val queue = new LinkedBlockingQueue[NSQMessage](1)
    val consumer = client.consumer(topic, consumer = queue.offer(_))

    try {
      fun(server, client, queue, consumer)
    } finally {
      consumer.close()
      client.close()
      server.close()
    }
  }
}
