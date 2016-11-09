package com.github.mitallast.nsq

import java.lang.Boolean
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}

import com.github.mitallast.nsq.protocol._
import com.sun.net.httpserver.Authenticator.Failure
import com.typesafe.config.{Config, ConfigFactory}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.{ByteBufAllocator, PooledByteBufAllocator, Unpooled}
import io.netty.channel._
import io.netty.channel.epoll.{Epoll, EpollEventLoopGroup, EpollSocketChannel}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.pool.{FixedChannelPool, _}
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.compression.{SnappyFramedDecoder, SnappyFramedEncoder, ZlibCodecFactory, ZlibWrapper}
import io.netty.handler.ssl.SslContextBuilder
import io.netty.util.concurrent.{DefaultThreadFactory, FutureListener, ScheduledFuture, Future ⇒ NettyFuture}
import io.netty.util.{AttributeKey, CharsetUtil}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{CancellationException, Future, Promise}
import scala.util.{Random, Success}

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

  override def channelRead0(ctx: ChannelHandlerContext, msg: NSQFrame) {
    val pipeline = ctx.channel().pipeline()
    val config = ctx.channel().attr(NSQConfig.attr).get()
    var reinstallDefaultDecoder = true

    msg match {
      case _: OK ⇒
        if (finished) {
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
          if (log.isDebugEnabled) {
            log.debug("adding ssl to pipeline")
          }
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

      case _ ⇒
        log.warn("unexpected identify response: {}", msg)
        ctx.fireChannelRead(msg)
    }
    ctx.channel().attr(NSQNettyClient.identifyAttr).get().complete(Success(Unit))
  }

  private def eject(reinstallDefaultDecoder: Boolean, pipeline: ChannelPipeline) = {
    pipeline.remove(this)
    if (reinstallDefaultDecoder) {
      if (pipeline.get(DEFLATE_DECODER) != null) {
        if (log.isDebugEnabled) {
          log.debug("remove deflate decoder")
        }
        pipeline.remove(DEFLATE_DECODER)
      }
      if (pipeline.get(SNAPPY_DECODER) != null) {
        if (log.isDebugEnabled) {
          log.debug("remove snappy decoder")
        }
        pipeline.remove(SNAPPY_DECODER)
      }
    }
  }

  private def installDeflateDecoder(pipeline: ChannelPipeline): Boolean = {
    finished = true
    if (log.isDebugEnabled) {
      log.debug("adding deflate to pipeline")
    }
    val decoder = ZlibCodecFactory.newZlibDecoder(ZlibWrapper.NONE)
    pipeline.addBefore(NSQ_DECODER, DEFLATE_DECODER, decoder)
    false
  }

  private def installSnappyDecoder(pipeline: ChannelPipeline): Boolean = {
    finished = true
    if (log.isDebugEnabled) {
      log.debug("adding snappy to pipeline")
    }
    val decoder = new SnappyFramedDecoder()
    pipeline.replace(NSQ_DECODER, SNAPPY_DECODER, decoder)
    false
  }

  private def parseIdentify(message: String): Unit = {
    if (message.equals("OK")) {
      return
    }
    else if (message.startsWith("{")) {
      if (message.contains(""""tls_v1":true""")) {
        ssl = true
      }
      if (message.contains(""""snappy":true""")) {
        snappy = true
        compression = true
      }
      if (message.contains(""""deflate":true""")) {
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
  val messagesAttr = AttributeKey.valueOf[AtomicLong]("nsq-messages")
  val consumerAttr = AttributeKey.valueOf[NSQMessage ⇒ Unit]("nsq-consumer")
  val identifyAttr = AttributeKey.valueOf[Promise[Unit]]("nsq-identify")
  val heartbeatAttr = AttributeKey.valueOf[Long]("nsq-heartbeat")
}

class NSQNettyClient(private val lookup: NSQLookup, private val config: Config) extends NSQClient {

  config.checkValid(ConfigFactory.defaultReference(), "nsq")

  private[nsq] val V2 = "  V2".getBytes(CharsetUtil.US_ASCII)
  private[nsq] val log = LoggerFactory.getLogger(NSQNettyClient.getClass)

  private[nsq] val epoll: Boolean = config.getBoolean("nsq.epoll")

  private[nsq] val threads: Int = config.getInt("nsq.threads")
  private[nsq] val keepAlive: Boolean = config.getBoolean("nsq.keep-alive")
  private[nsq] val reuseAddress: Boolean = config.getBoolean("nsq.reuse-address")
  private[nsq] val tcpNoDelay: Boolean = config.getBoolean("nsq.tcp-no-delay")
  private[nsq] val sndBuf: Int = config.getInt("nsq.snd-buf")
  private[nsq] val rcvBuf: Int = config.getInt("nsq.rcv-buf")
  private[nsq] val wbLow: Int = config.getInt("nsq.wb-low")
  private[nsq] val wbHigh: Int = config.getInt("nsq.wb-high")
  private[nsq] val maxConnections: Int = config.getInt("nsq.max-connections")
  private[nsq] val maxReadyCount: Int = config.getInt("nsq.max-ready-count")
  private[nsq] val lookupPeriod = config.getDuration("nsq.lookup-period", MILLISECONDS)

  private[nsq] val nsqConfig = {
    var conf = NSQConfig.default
    if (config.hasPath("nsq.identify.feature-negotiation")) {
      conf = conf.copy(featureNegotiation = config.getBoolean("nsq.identify.feature-negotiation"))
    }
    if (config.hasPath("nsq.identify.heartbeat-interval")) {
      conf = conf.copy(heartbeatInterval = Some(config.getInt("nsq.identify.heartbeat-interval")))
    }
    if (config.hasPath("nsq.identify.output-buffer-size")) {
      conf = conf.copy(outputBufferSize = Some(config.getInt("nsq.identify.output-buffer-size")))
    }
    if (config.hasPath("nsq.identify.output-buffer-timeout")) {
      conf = conf.copy(outputBufferTimeout = Some(config.getDuration("nsq.identify.output-buffer-timeout", TimeUnit.MILLISECONDS)))
    }
    if (config.hasPath("nsq.identify.tls-v1")) {
      conf = conf.copy(tlsV1 = Some(config.getBoolean("nsq.identify.tls-v1")))
    }
    if (config.hasPath("nsq.identify.snappy")) {
      conf = conf.copy(snappy = Some(config.getBoolean("nsq.identify.snappy")))
    }
    if (config.hasPath("nsq.identify.deflate")) {
      conf = conf.copy(deflate = Some(config.getBoolean("nsq.identify.deflate")))
    }
    if (config.hasPath("nsq.identify.deflate-level")) {
      conf = conf.copy(deflateLevel = Some(config.getInt("nsq.identify.deflate-level")))
    }
    if (config.hasPath("nsq.identify.sample-rate")) {
      conf = conf.copy(sampleRate = Some(config.getInt("nsq.identify.sample-rate")))
    }
    if (config.hasPath("nsq.identify.msg-timeout")) {
      conf = conf.copy(msgTimeout = Some(config.getDuration("nsq.identify.msg-timeout", TimeUnit.MILLISECONDS)))
    }
    conf
  }

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

    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
      if (log.isDebugEnabled) {
        log.debug("channel registered {}", ctx.channel())
      }
      val responses = new ConcurrentLinkedQueue[(NSQCommand, Promise[NSQFrame])]()
      ctx.channel().attr(NSQConfig.attr).set(nsqConfig)
      ctx.channel().attr(NSQNettyClient.responsesAttr).set(responses)
      ctx.channel().attr(NSQNettyClient.messagesAttr).set(new AtomicLong())
      ctx.channel().attr(NSQNettyClient.identifyAttr).set(Promise())
    }


    override def channelActive(ctx: ChannelHandlerContext): Unit = {
      if (log.isDebugEnabled) {
        log.debug("channel active {}", ctx.channel())
      }
      ctx.writeAndFlush(Unpooled.wrappedBuffer(V2))
      ctx.writeAndFlush(IdentifyCommand(nsqConfig))
      heartbeat(ctx)
      scheduleHeartbeat(ctx)
    }

    def scheduleHeartbeat(ctx: ChannelHandlerContext): Unit = {
      val heartbeatInterval = ctx.channel().attr(NSQConfig.attr).get().heartbeatInterval
      if (heartbeatInterval.exists(_ > 0)) {
        if (log.isTraceEnabled()) {
          log.trace("schedule heartbeat {}ms", heartbeatInterval.get)
        }
        ctx.executor().schedule(new Runnable {
          override def run() = {
            if (ctx.channel().isRegistered) {
              val heartbeatInterval = ctx.channel().attr(NSQConfig.attr).get().heartbeatInterval
              val heartbeat = ctx.channel().attr(NSQNettyClient.heartbeatAttr).get()
              if (heartbeatInterval.exists(_ > 0)) {
                if (heartbeat < (System.currentTimeMillis() - (heartbeatInterval.get * 2))) {
                  log.warn("heartbeat timeout at {}", ctx.channel())
                  ctx.channel().close()
                } else {
                  scheduleHeartbeat(ctx)
                }
              }
            }
          }
        }, heartbeatInterval.get, TimeUnit.MILLISECONDS)
      }
    }

    def heartbeat(ctx: ChannelHandlerContext) = {
      val heartbeatInterval = ctx.channel().attr(NSQConfig.attr).get().heartbeatInterval
      if (heartbeatInterval.exists(_ > 0)) {
        ctx.channel().attr(NSQNettyClient.heartbeatAttr).set(System.currentTimeMillis())
      }
    }

    override def channelInactive(ctx: ChannelHandlerContext) = {
      if (log.isDebugEnabled) {
        log.debug("channel inactive {}", ctx.channel())
      }
      super.channelInactive(ctx)
      ctx.channel().close()
      val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
      while (!responses.isEmpty) {
        responses.poll() match {
          case null ⇒ // ignore
          case (cmd, promise) ⇒
            log.warn("failed task {}", cmd)
            promise.failure(NSQDisconnected("channel closed"))
        }
      }
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) = {
      log.warn("exception caught", cause)
      ctx.channel().close()
      val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
      while (responses.nonEmpty) {
        responses.poll() match {
          case null ⇒ // ignore
          case (cmd, promise) ⇒
            log.warn("failed task {}", cmd)
            promise.failure(cause)
        }
      }
    }

    override def channelRead0(ctx: ChannelHandlerContext, frame: NSQFrame) = {
      if (log.isTraceEnabled) {
        log.trace("frame: {}", frame)
      }
      frame match {
        case HeartbeatFrame ⇒
          heartbeat(ctx)
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

        case ErrorFrame(error) ⇒
          val responses = ctx.channel().attr(NSQNettyClient.responsesAttr).get()
          (error, responses.poll()) match {
            // handle specific errors related to commands
            case (_: NSQErrorBadBody, (_: IdentifyCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadBody, (_: MPubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadMessage, (_: PubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadMessage, (_: MPubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadTopic, (_: SubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadTopic, (_: PubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadTopic, (_: MPubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadChannel, (_: SubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadChannel, (_: PubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorBadChannel, (_: MPubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorPubFailed, (_: PubCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorMpubFailed, (_: MPubCommand, promise)) ⇒ promise.failure(error)

            // FIN, REQ, TOUCH commands have not promises on write
            case (_: NSQErrorFinFailed, (_: FinCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorReqFailed, (_: ReqCommand, promise)) ⇒ promise.failure(error)
            case (_: NSQErrorTouchFailed, (_: TouchCommand, promise)) ⇒ promise.failure(error)

            // should close channel on fatal errors and failure current promise
            case (_: NSQError, (_, promise)) ⇒
              promise.failure(error)
              exceptionCaught(ctx, error)

            // should close channel on fatal errors
            case _ ⇒
              exceptionCaught(ctx, error)
          }

        case message: MessageFrame ⇒
          val messages = ctx.channel().attr(NSQNettyClient.messagesAttr).get()
          val received = messages.incrementAndGet()
          if (received % maxReadyCount > (maxReadyCount / 2)) {
            if (log.isTraceEnabled) {
              log.trace("send rdy {} to {}", maxReadyCount, ctx.channel().remoteAddress())
            }
            ctx.writeAndFlush(RdyCommand(maxReadyCount))
          }

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
    if (log.isDebugEnabled) {
      log.debug("shutdown")
    }
    bootstrap.group.shutdownGracefully.awaitUninterruptibly()
    if (log.isDebugEnabled) {
      log.debug("closed")
    }
  }

  private[nsq] def command(channel: Channel, cmd: NSQCommand): Future[NSQFrame] = {
    val promise = Promise[NSQFrame]()
    channel.eventLoop().execute(new Runnable {
      override def run() = {
        if (log.isTraceEnabled) {
          log.trace("command: {}", cmd)
        }
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

  def producer(): NSQProducer =
    new NSQNettyProducer()

  def consumer(topic: String, channel: String = "default")(consumer: NSQMessage ⇒ Unit): NSQConsumer =
    new NSQNettyConsumer(topic, channel, consumer)

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

    def req(timeout: Duration = 0.milliseconds) = {
      cancelTouch()
      ctx.writeAndFlush(ReqCommand(messageId, timeout.toMillis.toInt))
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

  private sealed trait NSQNettyPool {

    @volatile private var closed = false

    private val poolMap = new AbstractChannelPoolMap[SocketAddress, FixedChannelPool] {
      override def newPool(key: SocketAddress): FixedChannelPool = {
        new FixedChannelPool(bootstrap.remoteAddress(key), new AbstractChannelPoolHandler {
          override def channelCreated(channel: Channel): Unit = {
            if (log.isTraceEnabled) {
              log.trace("channel created: {}", key)
            }
            val pipeline = channel.pipeline
            pipeline.addLast("nsq-decoder", new NSQDecoder())
            pipeline.addLast("nsq-encoder", new NSQEncoder())
            pipeline.addLast("nsq-identity-handler", new NSQIdentifyHandler())
            pipeline.addLast("nsq-handler", new NSQChannelInboundHandler())
            channel.closeFuture().addListener(new FutureListener[Void] {
              override def operationComplete(future: NettyFuture[Void]) = {
                reconnect(key)
              }
            })
          }
        }, new NSQChannelHealthChecker, null, -1, maxConnections, Integer.MAX_VALUE, true)
      }
    }

    def connection[T](consumer: Channel ⇒ Future[T]): Future[T] = {
      val promise = Promise[T]()
      val pools = poolMap.iterator().map(_.getValue).toList
      val pool = pools.get(Random.nextInt(pools.size))
      val channelFuture = pool.acquire()
      if (channelFuture.isDone) {
        if (channelFuture.isSuccess) {
          val channel = channelFuture.getNow
          try {
            val identify = channel.attr(NSQNettyClient.identifyAttr).get()
            promise.completeWith(identify.future.flatMap(_ ⇒ consumer(channel)))
          } finally {
            pool.release(channel)
          }
        } else if (channelFuture.isCancelled) {
          promise.failure(new CancellationException())
        } else {
          promise.failure(channelFuture.cause())
        }
      } else {
        channelFuture.addListener(new FutureListener[Channel] {
          override def operationComplete(future: NettyFuture[Channel]) = {
            if (future.isSuccess) {
              val channel = future.getNow
              try {
                val identify = channel.attr(NSQNettyClient.identifyAttr).get()
                promise.completeWith(identify.future.flatMap(_ ⇒ consumer(channel)))
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
      }
      promise.future
    }

    private def connect(address: SocketAddress): Unit = {
      val pool = poolMap.get(address)
      val channelFuture = pool.acquire()
      if (channelFuture.isDone) {
        if (channelFuture.isSuccess) {
          val channel = channelFuture.getNow
          if (log.isDebugEnabled) {
            log.debug(s"successfully connected to {}", address)
          }
          channelCreated(channel)
          pool.release(channel)
        } else if (channelFuture.isCancelled) {
          log.warn(s"error connect to {}, canceled", address)
        } else {
          log.error(s"error connect to $address", channelFuture.cause())
        }
      } else {
        channelFuture.addListener(new FutureListener[Channel] {
          override def operationComplete(future: NettyFuture[Channel]) = {
            if (future.isSuccess) {
              val channel = future.getNow
              if (log.isDebugEnabled) {
                log.debug(s"successfully connected to {}", address)
              }
              channelCreated(channel)
              pool.release(channel)
            } else if (future.isCancelled) {
              log.warn(s"error connect to {}, canceled", address)
            } else {
              log.error(s"error connect to $address", future.cause())
            }
          }
        }).awaitUninterruptibly()
      }
    }

    private def reconnect(address: SocketAddress): Unit = {
      Future {
        if (!closed) {
          log.warn("reconnect")
          connect(address)
        }
      }
    }

    def put(address: SocketAddress): Unit = {
      if (!poolMap.contains(address)) {
        connect(address)
      }
    }

    def channelCreated(channel: Channel): Unit = {}

    def close(): Unit = {
      closed = true
      poolMap.close()
    }

    private class NSQChannelHealthChecker extends ChannelHealthChecker {
      override def isHealthy(channel: Channel): NettyFuture[Boolean] = {
        if (log.isTraceEnabled) {
          log.trace("check health")
        }
        val loop: EventLoop = channel.eventLoop
        if (!channel.isActive) {
          return loop.newSucceededFuture(Boolean.FALSE)
        }
        val heartbeatInterval = channel.attr(NSQConfig.attr).get().heartbeatInterval
        if (heartbeatInterval.exists(_ > 0)) {
          val heartbeat = channel.attr(NSQNettyClient.heartbeatAttr).get()
          if (heartbeat < (System.currentTimeMillis() - (heartbeatInterval.get * 2))) {
            log.warn("heartbeat timeout")
            return loop.newSucceededFuture(Boolean.FALSE)
          }
        }
        loop.newSucceededFuture(Boolean.TRUE)
      }
    }
  }

  private class NSQNettyProducer extends NSQProducer with NSQNettyPool {

    lookup.nodes().foreach(put)

    private val lookupTask = bootstrap.group().scheduleWithFixedDelay(new Runnable {
      override def run() = lookup.nodes().foreach(put)
    }, lookupPeriod, lookupPeriod, MILLISECONDS)

    override def close() = {
      lookupTask.cancel(true)
      super.close()
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

  private class NSQNettyConsumer(topic: String, channel: String = "default", consumer: NSQMessage ⇒ Unit)
    extends NSQConsumer with NSQNettyPool {

    lookup.lookup(topic).foreach(put)

    private val lookupTask = bootstrap.group().scheduleWithFixedDelay(new Runnable {
      override def run() = lookup.nodes().foreach(put)
    }, lookupPeriod, lookupPeriod, MILLISECONDS)

    override def channelCreated(ch: Channel): Unit = {
      ch.attr(NSQNettyClient.identifyAttr).get().future.onComplete {
        case Success(_) ⇒
          ch.attr(NSQNettyClient.consumerAttr).set(consumer)
          if (log.isTraceEnabled()) {
            log.trace("send sub {}:{} to {}", topic, channel, ch.remoteAddress())
          }
          val subCommand = SubCommand(topic, channel)
          ch.attr(NSQNettyClient.responsesAttr).get().add((subCommand, Promise[NSQFrame]()))
          ch.writeAndFlush(subCommand, ch.voidPromise())
          if (log.isTraceEnabled()) {
            log.info("send rdy {} to {}", maxReadyCount, ch.remoteAddress())
          }
          ch.writeAndFlush(RdyCommand(maxReadyCount), ch.voidPromise())
        case _ ⇒
      }
    }

    override def close() = {
      lookupTask.cancel(true)
      super.close()
    }
  }
}


