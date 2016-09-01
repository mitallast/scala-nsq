package org.mitallast.nsq.protocol

import java.nio.charset.Charset
import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.ReplayingDecoder
import io.netty.util.CharsetUtil
import org.mitallast.nsq._
import org.slf4j.LoggerFactory


private[nsq] sealed trait STATE
private[nsq] case object HEADER extends STATE
private[nsq] case object BODY extends STATE

private[nsq] class NSQDecoder extends ReplayingDecoder[STATE](HEADER) {

  val log = LoggerFactory.getLogger(getClass)

  var size: Int = 0
  var id: Int = 0

  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: util.List[AnyRef]) = {
    state() match {
      case HEADER ⇒
        size = in.readInt()
        id = in.readInt()
        checkpoint(BODY)
      case BODY ⇒
        val frame = id match {
          case 0 ⇒
            //subtract 4 because the frame id is included
            val pos = in.readerIndex()

            if (size - 4 == 2 && isOK(in)) {
              OK()
            }
            else if (size - 4 == 10 && isCloseWait(in)) {
              CloseWait
            }
            else if (size - 4 == 11 && isHeartbeat(in)) {
              Heartbeat
            }
            else {
              val data = new Array[Byte](size - 4)
              in.readBytes(data)
              ResponseFrame(data)
            }
          case 1 ⇒
            //subtract 4 because the frame id is included
            val error = in.readSlice(size - 4).toString(CharsetUtil.US_ASCII)
            ErrorFrame(error)
          case 2 ⇒
            val timestamp = in.readLong()
            val attempts = in.readUnsignedShort()
            val messageId = in.readSlice(16).toString(CharsetUtil.US_ASCII)
            //subtract 4 because the frame id is included
            val data = new Array[Byte](size - 4 - 8 - 2 - 16)
            in.readBytes(data)
            MessageFrame(timestamp, attempts, messageId, data)
          case _ ⇒
            throw new ProtocolException(s"bad frame id from server: [$id]")
        }
        checkpoint(HEADER)
        log.info("frame: {}", frame)
        out.add(frame)
    }
  }

  private def isOK(buf: ByteBuf): Boolean = {
    buf.markReaderIndex()
    if (buf.readByte() == 'O' && buf.readByte() == 'K') {
      true
    } else {
      buf.resetReaderIndex()
      false
    }
  }

  private def isCloseWait(buf: ByteBuf): Boolean = {
    buf.markReaderIndex()
    val equal = "CLOSE_WAIT".foldLeft(true) { case (b, ch) ⇒ b && buf.readByte() == ch }
    if (equal) {
      true
    } else {
      buf.resetReaderIndex()
      false
    }
  }

  private def isHeartbeat(buf: ByteBuf): Boolean = {
    buf.markReaderIndex()
    val equal = "_heartbeat_".foldLeft(true) { case (b, ch) ⇒ b && buf.readByte() == ch }
    if (equal) {
      true
    } else {
      buf.resetReaderIndex()
      false
    }
  }
}
