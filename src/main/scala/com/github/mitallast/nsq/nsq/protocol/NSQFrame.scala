package com.github.mitallast.nsq.nsq.protocol

import java.util

import io.netty.buffer.Unpooled
import io.netty.util.CharsetUtil
import com.github.mitallast.nsq.nsq.NSQError

private [nsq] sealed trait NSQFrame
private [nsq] sealed trait NSQResponseFrame extends NSQFrame

private [nsq] case class OK() extends NSQResponseFrame
private [nsq] case object Heartbeat extends NSQResponseFrame
private [nsq] case object CloseWait extends NSQResponseFrame

private [nsq] case class ResponseFrame(data: Array[Byte]) extends NSQResponseFrame {
  lazy val message = Unpooled.wrappedBuffer(data).toString(CharsetUtil.UTF_8)


  override def canEqual(that: Any): Boolean = that.isInstanceOf[ResponseFrame]

  override def equals(that: scala.Any): Boolean = {
    that match {
      case that: ResponseFrame ⇒ that.canEqual(that) && util.Arrays.equals(that.data, data)
      case _ ⇒ false
    }
  }
}

private [nsq] case class ErrorFrame(error: NSQError) extends NSQFrame

private [nsq] case class MessageFrame(timestamp: Long, attempts: Int, messageId: String, data: Array[Byte]) extends NSQFrame