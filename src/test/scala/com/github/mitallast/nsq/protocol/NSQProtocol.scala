package com.github.mitallast.nsq.protocol

import com.typesafe.config.ConfigFactory
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.local.LocalAddress
import io.netty.util.CharsetUtil
import org.slf4j.LoggerFactory

object NSQProtocol {

  val log = LoggerFactory.getLogger(getClass)
  val config = ConfigFactory.load("scala-nsq")
  val localAddr = new LocalAddress("nsq.id")
  val json ="""{"client_id":"test","hostname":"localhost","feature_negotiation":true,"user_agent":"test"}"""
  val timestamp = System.currentTimeMillis()
  val attempts = 2
  val messageId = "WCKHEOWCMPWECHWQ"
  val topic = "scala.nsq.test"

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

  def errorBuf(data: String) = {
    val b = buf(data)
    buf(buf(b.readableBytes() + 4), buf(1), b)
  }

  def messageBuf(data: String): ByteBuf = messageBuf(buf(data))

  def messageBuf(data: ByteBuf): ByteBuf = Unpooled.buffer()
    .writeInt(4 + 8 + 2 + 16 + data.readableBytes())
    .writeInt(2)
    .writeLong(timestamp)
    .writeShort(attempts)
    .writeBytes(messageId.getBytes(CharsetUtil.US_ASCII))
    .writeBytes(data)


}