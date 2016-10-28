package com.github.mitallast.nsq

import java.io.Closeable

import com.typesafe.config.{Config, ConfigFactory}
import io.netty.util.CharsetUtil

import scala.concurrent.Future
import scala.concurrent.duration.Duration

trait OK

trait NSQMessage {

  def timestamp: Long

  def attempts: Int

  def messageId: String

  def data: Array[Byte]

  def fin(): Unit

  def req(timeout: Int): Unit

  def touch(): Unit

  def touch(duration: Duration): Unit
}

trait NSQClient extends Closeable {

  def producer(): NSQProducer

  def consumer(topic: String, channel: String = "default")(consumer: NSQMessage ⇒ Unit): NSQConsumer
}

object NSQClient {

  def apply(): NSQClient = apply(ConfigFactory.load())

  def apply(config: Config): NSQClient = apply(NSQLookup(config), config)

  def apply(lookup: NSQLookup): NSQClient = apply(lookup, ConfigFactory.load())

  def apply(lookup: NSQLookup, config: Config): NSQClient = new NSQNettyClient(lookup, config.withFallback(ConfigFactory.defaultReference()))
}

trait NSQProducer extends Closeable {

  def pub(topic: String, data: Array[Byte]): Future[OK]

  def pubStr(topic: String, data: String): Future[OK] = {
    pub(topic, data.getBytes(CharsetUtil.UTF_8))
  }

  def mpub(topic: String, data: Seq[Array[Byte]]): Future[OK]

  def mpubStr(topic: String, data: Seq[String]): Future[OK] = {
    mpub(topic, data.map(_.getBytes(CharsetUtil.UTF_8)))
  }
}

trait NSQConsumer extends Closeable {
}