package org.mitallast.nsq.protocol

import java.nio.charset.Charset

import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil

private [nsq] sealed trait NSQFrame
private [nsq] sealed trait NSQResponseFrame extends NSQFrame

private [nsq] case class OK() extends NSQResponseFrame
private [nsq] case object Heartbeat extends NSQResponseFrame
private [nsq] case object CloseWait extends NSQResponseFrame

private [nsq] case class ResponseFrame(data: Array[Byte]) extends NSQResponseFrame {
  lazy val message = Unpooled.wrappedBuffer(data).toString(CharsetUtil.UTF_8)
}

private [nsq] case class ErrorFrame(message: String) extends NSQFrame

private [nsq] case class MessageFrame(timestamp: Long, attempts: Int, messageId: String, data: Array[Byte]) extends NSQFrame