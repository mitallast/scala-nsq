package org.mitallast.nsq

import java.io.Closeable

import io.netty.util.CharsetUtil
import org.mitallast.nsq.protocol.OK

import scala.concurrent.Future

trait NSQMessage {

  def timestamp: Long

  def attempts: Int

  def messageId: String

  def data: Array[Byte]

  def fin(): Unit

  def req(timeout: Int): Unit

  def touch(): Unit
}

trait NSQClient extends Closeable {

  def producer(): NSQProducer

  def consumer(topic: String, channel: String = "default", consumer: NSQMessage ⇒ Unit): NSQConsumer
}


trait NSQProducer extends Closeable {

  def connect(host: String, port: Int): Unit

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

  def connect(host: String, port: Int): Unit

  def ready(count: Int): Unit

  def readyAll(count: Int): Unit
}