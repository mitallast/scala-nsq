package org.mitallast.nsq

import java.net.SocketAddress
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator, Unpooled}
import io.netty.channel._
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.pool._
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.compression.{SnappyFramedDecoder, SnappyFramedEncoder, ZlibCodecFactory, ZlibWrapper}
import io.netty.handler.ssl.SslContextBuilder
import io.netty.util.concurrent.{DefaultThreadFactory, FutureListener, ScheduledFuture, Future ⇒ NettyFuture}
import io.netty.util.{AttributeKey, CharsetUtil}
import org.mitallast.nsq.protocol._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.{CancellationException, Future, Promise}
import scala.util.Random
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration.Duration

private[nsq] class NSQIdentifyHandler extends SimpleChannelInboundHandler[NSQFrame] {

  val SSL_HANDLER = "nsq-ssl-handler"
  val SNAPPY_ENCODER = "nsq-snappy-encoder"
  val DEFLATE_ENCODER = "nsq-deflate-encoder"
  val NSQ_DECODER = "nsq-decoder"
  val NSQ_ENCODER = "nsq-encoder"
  val DEFLATE_DECODER = "nsq-deflate-decoder"
  val SNAPPY_DECODER = "nsq-snappy-decoder"

  val log = LoggerFactory.getLogger(getClass)

  private var ssl: Boolean = false
  private var compression: Boolean = false
  private var snappy: Boolean = false
  private var deflate: Boolean = false
  private var finished: Boolean = false

  private implicit val formats = DefaultFormats

  override def channelRead0(ctx: ChannelHandlerContext, msg: NSQFrame) {
    log.info("frame: {}", msg)
    var reinstallDefaultDecoder = true
    msg match {
      case frame: NSQResponseFrame ⇒

        val pipeline = ctx.channel().pipeline()
        val config = ctx.channel().attr(NSQConfig.attr).get()

        frame match {
          case _: OK ⇒
            if (finished) {
              ctx.fireChannelRead(msg)
              return
            }
            //round 2
            if (snappy) {
              reinstallDefaultDecoder = installSnappyDecoder(pipeline)
            }
            if (deflate) {
              reinstallDefaultDecoder = installDeflateDecoder(pipeline)
            }
            eject(reinstallDefaultDecoder, pipeline)

          case response: ResponseFrame ⇒

            val message = response.message
            parseIdentify(message)

            if (ssl) {
              log.info("adding ssl to pipeline")
              val sslContext = SslContextBuilder.forClient().build()
              val sslHandler = sslContext.newHandler(ctx.channel().alloc())
              pipeline.addBefore(NSQ_DECODER, SSL_HANDLER, sslHandler)
              if (snappy) {
                pipeline.addBefore(NSQ_ENCODER, SNAPPY_ENCODER, new SnappyFramedEncoder())
              }
              if (deflate) {
                pipeline.addBefore(NSQ_ENCODER, DEFLATE_ENCODER, ZlibCodecFactory.newZlibEncoder(
                  ZlibWrapper.NONE,
                  config.deflateLevel.getOrElse(6)))
              }
            }
            if (!ssl && snappy) {
              pipeline.addBefore(NSQ_ENCODER, SNAPPY_ENCODER, new SnappyFramedEncoder())
              reinstallDefaultDecoder = installSnappyDecoder(pipeline)
            }
            if (!ssl && deflate) {
              pipeline.addBefore(NSQ_ENCODER, DEFLATE_ENCODER, ZlibCodecFactory.newZlibEncoder(
                ZlibWrapper.NONE,
                config.deflateLevel.getOrElse(6)))
              reinstallDefaultDecoder = installDeflateDecoder(pipeline)
            }
            if (message.contains("version") && finished) {
              eject(reinstallDefaultDecoder, pipeline)
            }

          case _ ⇒ ctx.fireChannelRead(msg)
        }

      case _ ⇒ ctx.fireChannelRead(msg)
    }
  }

  private def eject(reinstallDefaultDecoder: Boolean, pipeline: ChannelPipeline) = {
    pipeline.remove(this)
    if (reinstallDefaultDecoder) {
      if (pipeline.get(DEFLATE_DECODER) != null) {
        log.info("remove deflate decoder")
        pipeline.remove(DEFLATE_DECODER)
      }
      if (pipeline.get(SNAPPY_DECODER) != null) {
        log.info("remove snappy decoder")
        pipeline.remove(SNAPPY_DECODER)
      }
    }
  }

  private def installDeflateDecoder(pipeline: ChannelPipeline): Boolean = {
    finished = true
    log.info("adding deflate to pipeline")
    val decoder = ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE)
    pipeline.addBefore(NSQ_DECODER, DEFLATE_DECODER, decoder)
    false
  }

  private def installSnappyDecoder(pipeline: ChannelPipeline): Boolean = {
    finished = true
    log.info("adding snappy to pipeline")
    val decoder = new SnappyFramedDecoder()
    pipeline.replace(NSQ_DECODER, SNAPPY_DECODER, decoder)
    false
  }

  private def parseIdentify(message: String): Unit = {
    if (message.equals("OK")) {
      return
    }
    else if (message.startsWith("{")) {
      val json = parse(StringInput(message))

      def check(field: String) = json.findField {
        case JField("name", JBool.True) ⇒ true
        case _ ⇒ false
      }.nonEmpty

      if (check("tls_v1")) {
        ssl = true
      }
      if (check("snappy")) {
        snappy = true
        compression = true
      }
      if (check("deflate")) {
        deflate = true
        compression = true
      }
    }
    if (!ssl && !compression) {
      finished = true
    }
  }
}

private[nsq] case object NSQNettyClient {
  val responsesAttr = AttributeKey.valueOf[ConcurrentLinkedQueue[(NSQCommand, Promise[NSQFrame])]]("nsq-responses")
  val consumerAttr = AttributeKey.valueOf[NSQMessage ⇒ Unit]("nsq-consumer")
}

class NSQNettyClient(val config: Config) extends NSQClient {

  config.checkValid(ConfigFactory.defaultReference(), "scala-nsq")

  private[nsq] val V2 = "  V2".getBytes(CharsetUtil.US_ASCII)
  private[nsq] val log = LoggerFactory.getLogger(getClass)

  private[nsq] val epoll: Boolean = config.getBoolean("scala-nsq.epoll")

  private[nsq] val threads: Int = config.getInt("scala-nsq.threads")
  private[nsq] val keepAlive: Boolean = config.getBoolean("scala-nsq.keep-alive")
  private[nsq] val reuseAddress: Boolean = config.getBoolean("scala-nsq.reuse-address")
  private[nsq] val tcpNoDelay: Boolean = config.getBoolean("scala-nsq.tcp-no-delay")
  private[nsq] val sndBuf: Int = config.getInt("scala-nsq.snd-buf")
  private[nsq] val rcvBuf: Int = config.getInt("scala-nsq.rcv-buf")
  private[nsq] val wbLow: Int = config.getInt("scala-nsq.wb-low")
  private[nsq] val wbHigh: Int = config.getInt("scala-nsq.wb-high")
  private[nsq] val maxConnections: Int = config.getInt("scala-nsq.max-connections")
  private[nsq] val lookupAddressList: List[String] = config.getStringList("scala-nsq.lookup-address").toList

  private[nsq] val nsqConfig = NSQConfig.default

  private[nsq] val lookup = new NSQLookup(lookupAddressList)

  private[nsq] def newBootstrap = {
    val threadFactory = new DefaultThreadFactory("nsq-client", true)
    if (epoll && Epoll.isAvailable) {
      new Bootstrap()
        .channel(classOf[EpollSocketChannel])
        .group(new EpollEventLoopGroup(threads, threadFactory))
    } else {
      new Bootstrap()
        .channel(classOf[NioSocketChannel])
        .group(new NioEventLoopGroup(threads, threadFactory))
    }
  }

  private[nsq] val bootstrap: Bootstrap = newBootstrap
    .option[java.lang.Boolean](ChannelOption.SO_REUSEADDR, reuseAddress)
    .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, keepAlive)
    .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, tcpNoDelay)
    .option[java.lang.Integer](ChannelOption.SO_SNDBUF, sndBuf)
    .option[java.lang.Integer](ChannelOption.SO_RCVBUF, rcvBuf)
    .option[java.lang.Integer](ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, wbHigh)
    .option[java.lang.Integer](ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, wbLow)
    .option[ByteBufAllocator](ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
    .option[RecvByteBufAllocator](ChannelOption.RCVBUF_ALLOCATOR, AdaptiveRecvByteBufAllocator.DEFAULT)

  private[nsq] class NSQChannelInboundHandler extends SimpleChannelInboundHandler[NSQFrame] {

    override def isSharable = true

    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      log.info("channel active {}", ctx)
      super.channelActive(ctx)
      val responses = new ConcurrentLinkedQueue[(NSQCommand, Promise[NSQFrame])]()
      ctx.channel().attr(NSQConfig.attr).set(NSQConfig.default)
      ctx.channel().attr(NSQNettyClient.responsesAttr).set(responses)
      ctx.writeAndFlush(Unpooled.wrappedBuffer(V2))
      ctx.writeAndFlush(IdentifyCommand(nsqConfig))
    }

    override def channelInactive(ctx: ChannelHandlerContext) = {
      log.info("channel inactive {}", ctx)
      super.channelInactive(ctx)
      ctx.channel().close()
      val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
      while (!responses.isEmpty) {
        responses.poll() match {
          case null ⇒ // ignore
          case (cmd, promise) ⇒
            log.warn("cancel task {}", cmd)
            promise.failure(new CancellationException())
        }
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
      super.exceptionCaught(ctx, cause)
      log.warn("exception caught", cause)
      ctx.channel().close()
    }

    override def channelRead0(ctx: ChannelHandlerContext, frame: NSQFrame) = {
      log.info("frame: {}", frame)
      frame match {
        case Heartbeat ⇒
          ctx.writeAndFlush(NopCommand)

        case response: NSQResponseFrame ⇒
          val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
          var poll = true
          while (poll) {
            responses.poll() match {
              case null ⇒ // ignore
              case (cmd, promise) ⇒
                promise.success(response)
                poll = false
            }
          }

        case error: ErrorFrame ⇒
          val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
          responses.poll() match {
            case null ⇒ // ignore
            case (cmd, promise) ⇒
              promise.failure(new NSQException(error.message))
          }

        case message: MessageFrame ⇒
          val consumer = ctx.channel().attr(NSQNettyClient.consumerAttr).get()
          if (consumer != null) {
            val msg = NSQMessageImpl(
              timestamp = message.timestamp,
              attempts = message.attempts,
              messageId = message.messageId,
              data = message.data,
              ctx = ctx
            )
            consumer(msg)
          }
      }
    }
  }

  def close() = {
    log.info("shutdown")
    bootstrap.group.shutdownGracefully.awaitUninterruptibly()
    log.info("closed")
  }

  private[nsq] def command(channel: Channel, cmd: NSQCommand): Future[NSQFrame] = {
    val promise = Promise[NSQFrame]()
    channel.eventLoop().execute(new Runnable {
      override def run() = {
        log.info("command: {}", cmd)
        val responses = channel.attr(NSQNettyClient.responsesAttr).get()
        responses.add((cmd, promise))
        channel.writeAndFlush(cmd).addListener(new ChannelFutureListener {
          override def operationComplete(future: ChannelFuture) = {
            if (future.isCancelled) {
              promise.failure(new CancellationException())
            }
            else if (!future.isSuccess) {
              promise.failure(future.cause())
            }
          }
        })
      }
    })
    promise.future
  }

  def producer(): NSQProducer = {
    val nsqProducer = new NSQNettyProducer()
    lookup.nodes() foreach { address ⇒
      nsqProducer.connect(address)
    }
    nsqProducer
  }

  def consumer(topic: String, channel: String = "default", consumer: NSQMessage ⇒ Unit): NSQConsumer = {
    val nsqConsumer = new NSQNettyConsumer(topic, channel, consumer)
    lookup.lookup(topic) foreach { address ⇒
      nsqConsumer.connect(address)
    }
    nsqConsumer
  }

  private[nsq] case class NSQMessageImpl(
    timestamp: Long,
    attempts: Int,
    messageId: String,
    data: Array[Byte],
    ctx: ChannelHandlerContext
  ) extends NSQMessage {

    private[nsq] var touchTask: ScheduledFuture[_] = _

    def fin() = {
      cancelTouch()
      ctx.writeAndFlush(FinCommand(messageId))
    }

    def req(timeout: Int) = {
      cancelTouch()
      ctx.writeAndFlush(ReqCommand(messageId, timeout))
    }

    def touch() = {
      cancelTouch()
      ctx.writeAndFlush(TouchCommand(messageId))
    }

    def touch(duration: Duration) = {
      cancelTouch()
      touchTask = ctx.executor().scheduleAtFixedRate(new Runnable {
        override def run() = {
          ctx.writeAndFlush(TouchCommand(messageId))
        }
      }, duration.toMillis, duration.toMillis, TimeUnit.MILLISECONDS)
    }

    private def cancelTouch(): Unit = {
      if (touchTask != null) {
        touchTask.cancel(false)
      }
    }
  }

  private[nsq] class NSQNettyProducer extends NSQProducer {

    private val poolMap = new AbstractChannelPoolMap[SocketAddress, FixedChannelPool] {
      override def newPool(key: SocketAddress): FixedChannelPool = {
        new FixedChannelPool(bootstrap.remoteAddress(key), new AbstractChannelPoolHandler {
          override def channelCreated(ch: Channel): Unit = {
            log.info("channel created: {}", key)
            val pipeline = ch.pipeline
            pipeline.addLast("nsq-decoder", new NSQDecoder())
            pipeline.addLast("nsq-encoder", new NSQEncoder())
            pipeline.addLast("nsq-identity-handler", new NSQIdentifyHandler())
            pipeline.addLast("nsq-handler", new NSQChannelInboundHandler())
          }
        }, maxConnections)
      }
    }

    private[nsq] def connect(address: SocketAddress): Unit = {
      val pool = poolMap.get(address)
      pool.acquire().addListener(new FutureListener[Channel] {
        override def operationComplete(future: NettyFuture[Channel]) = {
          if (future.isSuccess) {
            val channel = future.getNow
            try {
              log.info("connected: {}", channel.remoteAddress())
            } finally {
              pool.release(channel)
            }
          }
        }
      }).awaitUninterruptibly()
    }

    private[nsq] def connection[T](consumer: Channel ⇒ Future[T]): Future[T] = {
      val promise = Promise[T]()
      val pools = poolMap.iterator().map(_.getValue).toList
      val pool = pools.get(Random.nextInt(pools.size))
      pool.acquire().addListener(new FutureListener[Channel] {
        override def operationComplete(future: NettyFuture[Channel]) = {
          if (future.isSuccess) {
            val channel = future.getNow
            try {
              promise.completeWith(consumer(channel))
            } finally {
              pool.release(channel)
            }
          } else if (future.isCancelled) {
            promise.failure(new CancellationException())
          } else {
            promise.failure(future.cause())
          }
        }
      })
      promise.future
    }

    def close() = {
      poolMap.close()
    }

    def pub(topic: String, data: Array[Byte]): Future[OK] = {
      connection { channel ⇒
        command(channel, PubCommand(topic, data)).mapTo[OK]
      }
    }

    def mpub(topic: String, data: Seq[Array[Byte]]): Future[OK] = {
      connection { channel ⇒
        command(channel, MPubCommand(topic, data)).mapTo[OK]
      }
    }
  }

  private[nsq] class NSQNettyConsumer(topic: String, channel: String = "default", consumer: NSQMessage ⇒ Unit) extends NSQConsumer {

    private[nsq] class NSQConsumerChannelInboundHandler extends NSQChannelInboundHandler {
      override def channelActive(ctx: ChannelHandlerContext): Unit = {
        super.channelActive(ctx)
        ctx.channel().attr(NSQNettyClient.consumerAttr).set(consumer)

        val command = SubCommand(topic, channel)
        val promise = Promise[NSQFrame]()

        ctx.channel().attr(NSQNettyClient.responsesAttr).get().add((command, promise))
        ctx.writeAndFlush(command)
      }
    }

    private val poolMap = new AbstractChannelPoolMap[SocketAddress, FixedChannelPool] {
      override def newPool(key: SocketAddress) = {
        new FixedChannelPool(bootstrap.remoteAddress(key), new AbstractChannelPoolHandler {
          override def channelCreated(ch: Channel): Unit = {
            log.info("channel created: {}", key)
            val pipeline = ch.pipeline
            pipeline.addLast("nsq-decoder", new NSQDecoder())
            pipeline.addLast("nsq-encoder", new NSQEncoder())
            pipeline.addLast("nsq-identity-handler", new NSQIdentifyHandler())
            pipeline.addLast("nsq-handler", new NSQConsumerChannelInboundHandler())
          }
        }, maxConnections)
      }
    }

    private[nsq] def connect(address: SocketAddress): Unit = {
      val pool = poolMap.get(address)
      pool.acquire()
        .addListener(new FutureListener[Channel] {
          override def operationComplete(future: NettyFuture[Channel]) = {
            if (future.isSuccess) {
              val channel = future.getNow
              log.info(s"successfully connected to {}", address)
              pool.release(channel)
            } else if (future.isCancelled) {
              log.warn(s"error connect to {}, canceled", address)
            } else {
              log.error(s"error connect to $address", future.cause())
            }
          }
        })
        .awaitUninterruptibly()
    }

    def close() = {
      poolMap.close()
    }

    def ready(count: Int): Unit = {
      val pools = poolMap.iterator().map(_.getValue).toList
      val pool = pools.get(Random.nextInt(pools.size))
      pool.acquire().addListener(new FutureListener[Channel] {
        override def operationComplete(future: NettyFuture[Channel]): Unit = {
          if (future.isSuccess) {
            val channel = future.getNow
            try {
              log.info("channel {} active={}", channel, channel.isActive)
              log.info("send rdy({}) to {}", count, channel.remoteAddress())
              channel.writeAndFlush(RdyCommand(count))
            } finally {
              pool.release(channel)
            }
          } else if (future.isCancelled) {
            log.warn("error acquire channel, canceled")
          } else {
            log.error("error acquire channel", future.cause())
          }
        }
      }).awaitUninterruptibly()
    }

    def readyAll(count: Int): Unit = {
      log.info("send rdy({}) to all", count)
      val iterator = poolMap.iterator()
      while (iterator.hasNext) {
        val entry = iterator.next()
        val pool = entry.getValue
        pool.acquire().addListener(new FutureListener[Channel] {
          override def operationComplete(future: NettyFuture[Channel]): Unit = {
            if (future.isSuccess) {
              val channel = future.getNow
              try {
                log.info("send rdy({}) to {}", count, channel.remoteAddress())
                channel.writeAndFlush(RdyCommand(count))
              } finally {
                pool.release(channel)
              }
            }
          }
        })
      }
    }
  }
}


