package com.github.mitallast.nsq.protocol

import io.netty.channel.embedded.EmbeddedChannel
import com.github.mitallast.nsq._
import org.scalatest.{FlatSpec, Matchers}

class NSQDecoderSpec extends FlatSpec with Matchers {

  import NSQProtocol._

  def decodeError(error: String) = {
    val channel = new EmbeddedChannel(new NSQDecoder)
    channel.writeInbound(errorBuf(error))
    throw channel.readInbound().asInstanceOf[ErrorFrame].error
  }

  "nsq decoder" should "decode E_INVALID" in {
    an[NSQErrorInvalid] should be thrownBy decodeError(NSQError.E_INVALID)
  }

  it should "decode E_BAD_BODY" in {
    an[NSQErrorBadBody] should be thrownBy decodeError(NSQError.E_BAD_BODY)
  }

  it should "decode E_BAD_TOPIC" in {
    an[NSQErrorBadTopic] should be thrownBy decodeError(NSQError.E_BAD_TOPIC)
  }

  it should "decode E_BAD_CHANNEL" in {
    an[NSQErrorBadChannel] should be thrownBy decodeError(NSQError.E_BAD_CHANNEL)
  }

  it should "decode E_PUB_FAILED" in {
    an[NSQErrorPubFailed] should be thrownBy decodeError(NSQError.E_PUB_FAILED)
  }

  it should "decode E_MPUB_FAILED" in {
    an[NSQErrorMpubFailed] should be thrownBy decodeError(NSQError.E_MPUB_FAILED)
  }

  it should "decode E_FIN_FAILED" in {
    an[NSQErrorFinFailed] should be thrownBy decodeError(NSQError.E_FIN_FAILED)
  }

  it should "decode E_REQ_FAILED" in {
    an[NSQErrorReqFailed] should be thrownBy decodeError(NSQError.E_REQ_FAILED)
  }

  it should "decode E_TOUCH_FAILED" in {
    an[NSQErrorTouchFailed] should be thrownBy decodeError(NSQError.E_TOUCH_FAILED)
  }

  it should "decode E_AUTH_FAILED" in {
    an[NSQErrorAuthFailed] should be thrownBy decodeError(NSQError.E_AUTH_FAILED)
  }

  it should "decode E_UNAUTHORIZED" in {
    an[NSQErrorUnauthorized] should be thrownBy decodeError(NSQError.E_UNAUTHORIZED)
  }

  it should "decode unexpected error" in {
    an[NSQProtocolError] should be thrownBy decodeError("UNEXPECTED")
  }

  it should "decode OK response" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    channel.writeInbound(responseBuf("OK"))
    channel.readInbound() shouldEqual OK()
  }

  it should "decode CLOSE_WAIT response" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    channel.writeInbound(responseBuf("CLOSE_WAIT"))
    channel.readInbound() shouldEqual CloseWait
  }

  it should "decode _heartbeat_ response" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    channel.writeInbound(responseBuf("_heartbeat_"))
    channel.readInbound() shouldEqual Heartbeat
  }

  it should "decode response" in {
    val channel = new EmbeddedChannel(new NSQDecoder)
    channel.writeInbound(responseBuf("hello"))
    val decode = channel.readInbound()
    decode.isInstanceOf[ResponseFrame] shouldBe true
    decode.asInstanceOf[ResponseFrame].data shouldEqual "hello".getBytes
  }
}
