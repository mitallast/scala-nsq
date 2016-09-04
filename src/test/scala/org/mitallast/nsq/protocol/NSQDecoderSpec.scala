package org.mitallast.nsq.protocol

import io.netty.channel.embedded.EmbeddedChannel
import org.mitallast.nsq._
import org.scalatest.{FlatSpec, Matchers}

class NSQDecoderSpec extends FlatSpec with Matchers {

  import NSQProtocol._

  "nsq decoder" should "decode E_INVALID" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorInvalid] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_INVALID))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_BAD_BODY" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorBadBody] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_BAD_BODY))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_BAD_TOPIC" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorBadTopic] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_BAD_TOPIC))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_BAD_CHANNEL" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorBadChannel] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_BAD_CHANNEL))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_PUB_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorPubFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_PUB_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_MPUB_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorMpubFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_MPUB_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_FIN_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorFinFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_FIN_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_REQ_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorReqFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_REQ_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_TOUCH_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorTouchFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_TOUCH_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_AUTH_FAILED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorAuthFailed] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_AUTH_FAILED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode E_UNAUTHORIZED" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQErrorUnauthorized] should be thrownBy {
      channel.writeInbound(errorBuf(NSQError.E_UNAUTHORIZED))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }

  it should "decode unexpected error" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    an[NSQProtocolException] should be thrownBy {
      channel.writeInbound(errorBuf("UNEXPECTED"))
      throw channel.readInbound().asInstanceOf[Exception]
    }
  }
}
