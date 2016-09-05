package com.github.mitallast.nsq.nsq.protocol

import java.util

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder

private[nsq] class NSQEncoder extends MessageToMessageEncoder[NSQCommand] {
  override def encode(ctx: ChannelHandlerContext, message: NSQCommand, out: util.List[AnyRef]) = {
    message match {
      case cmd: IdentifyCommand ⇒
        val buf = ctx.alloc().buffer(14)
        "IDENTIFY\n".foreach(buf.writeByte(_))
        val json = ctx.alloc().buffer()
        cmd.config.asJson(json)
        buf.writeInt(json.readableBytes())
        buf.writeBytes(json)
        json.release()
        out.add(buf)
      case cmd: SubCommand ⇒
        val size = cmd.topic.length + cmd.channel.length + 6
        val buf = ctx.alloc().buffer(size)
        "SUB ".foreach(buf.writeByte(_))
        cmd.topic.foreach(buf.writeByte(_))
        buf.writeByte(' ')
        cmd.channel.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        out.add(buf)
      case cmd: PubCommand ⇒
        val size = cmd.topic.length + 9 + cmd.data.length
        val buf = ctx.alloc().buffer(size)
        "PUB ".foreach(buf.writeByte(_))
        cmd.topic.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        buf.writeInt(cmd.data.length)
        buf.writeBytes(cmd.data)
        out.add(buf)
      case cmd: MPubCommand ⇒
        val bodySize = cmd.data.map(_.length + 4).sum + 4
        val size = cmd.topic.length + 10 + bodySize
        val buf = ctx.alloc().buffer(size)
        "MPUB ".foreach(buf.writeByte(_))
        cmd.topic.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        buf.writeInt(bodySize)
        buf.writeInt(cmd.data.length)
        cmd.data.foreach(data ⇒ buf.writeInt(data.length).writeBytes(data))
        out.add(buf)
      case cmd: RdyCommand ⇒
        val count = cmd.count.toString
        val size = count.length + 5
        val buf = ctx.alloc().buffer(size)
        "RDY ".foreach(buf.writeByte(_))
        count.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        out.add(buf)
      case cmd: FinCommand ⇒
        val size = cmd.messageId.length + 5
        val buf = ctx.alloc().buffer(size)
        "FIN ".foreach(buf.writeByte(_))
        cmd.messageId.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        out.add(buf)
      case cmd: ReqCommand ⇒
        val timeout = cmd.timeout.toString
        val size = cmd.messageId.length + timeout.length + 6
        val buf = ctx.alloc().buffer(size)
        "REQ ".foreach(buf.writeByte(_))
        cmd.messageId.foreach(buf.writeByte(_))
        buf.writeByte(' ')
        timeout.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        out.add(buf)
      case cmd: TouchCommand ⇒
        val size = cmd.messageId.length + 7
        val buf = ctx.alloc().buffer(size)
        "TOUCH ".foreach(buf.writeByte(_))
        cmd.messageId.foreach(buf.writeByte(_))
        buf.writeByte('\n')
        out.add(buf)
      case ClsCommand ⇒
        val buf = ctx.alloc().buffer(4)
        "CLS\n".foreach(buf.writeByte(_))
        out.add(buf)
      case NopCommand ⇒
        val buf = ctx.alloc().buffer(4)
        "NOP\n".foreach(buf.writeByte(_))
        out.add(buf)
      case cmd: AuthCommand ⇒
        val size = 6 + cmd.secret.length
        val buf = ctx.alloc().buffer(size)
        "AUTH\n".foreach(buf.writeByte(_))
        buf.writeInt(cmd.secret.length)
        buf.writeBytes(cmd.secret)
        out.add(buf)
    }
  }
}
