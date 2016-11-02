package com.github.mitallast.nsq.protocol

import java.net.InetAddress

import io.netty.buffer.ByteBuf
import io.netty.util.AttributeKey

/**
  *
  * @param clientId            an identifier used to disambiguate this client (ie. something specific to the consumer)
  * @param hostname            the hostname where the client is deployed
  * @param userAgent           (nsqd v0.2.25+) a string identifying the agent for this client in the spirit of HTTP
  *                            Default: <client_library_name>/<version>
  * @param snappy              snappy (nsqd v0.2.23+) enable snappy compression for this connection.
  *                            --snappy (nsqd flag) enables support for this server side
  * @param deflate             deflate (nsqd v0.2.23+) enable deflate compression for this connection.
  *                            --deflate (nsqd flag) enables support for this server side
  *
  *                            The client should expect an additional, deflate compressed OK response immediately after
  *                            the IDENTIFY response.
  *                            A client cannot enable both snappy and deflate.
  * @param featureNegotiation  (nsqd v0.2.19+) bool used to indicate that the client supports feature negotiation.
  *                            If the server is capable, it will send back a JSON payload of supported features
  *                            and metadata.
  * @param heartbeatInterval   (nsqd v0.2.19+) milliseconds between heartbeats.
  *                            Valid range: 1000 <= heartbeat_interval <= configured_max (-1 disables heartbeats)
  *                            --max-heartbeat-interval (nsqd flag) controls the max
  *                            Defaults to --client-timeout / 2
  * @param outputBufferSize    (nsqd v0.2.21+) the size in bytes of the buffer nsqd will use when writing to this client.
  *                            Valid range: 64 <= output_buffer_size <= configured_max (-1 disables output buffering)
  *                            --max-output-buffer-size (nsqd flag) controls the max
  *                            Defaults to 16kb
  * @param outputBufferTimeout (nsqd v0.2.21+) the timeout after which any data that nsqd has buffered will be flushed
  *                            to this client. Valid range: 1ms <= output_buffer_timeout <= configured_max (-1 disables
  *                            timeouts) --max-output-buffer-timeout (nsqd flag) controls the max. Defaults to 250ms
  *                            Warning: configuring clients with an extremely low (< 25ms) output_buffer_timeout has
  *                            a significant effect on nsqd CPU usage (particularly with > 50 clients connected).
  *                            This is due to the current implementation relying on Go timers which are maintained by
  *                            the Go runtime in a priority queue. See the commit message in pull request #236
  *                            for more details.
  * @param tlsV1               (nsqd v0.2.22+) enable TLS for this connection.
  *                            --tls-cert and --tls-key (nsqd flags) enable TLS and configure the server certificate
  *                            If the server supports TLS it will reply "tls_v1": true
  *                            The client should begin the TLS handshake immediately after reading the IDENTIFY response
  *                            The server will respond OK after completing the TLS handshake
  * @param deflateLevel        (nsqd v0.2.23+) configure the deflate compression level for this connection.
  *                            --max-deflate-level (nsqd flag) configures the maximum allowed value
  *                            Valid range: 1 <= deflate_level <= configured_max
  *                            Higher values mean better compression but more CPU usage for nsqd.
  * @param sampleRate          (nsqd v0.2.25+) deliver a percentage of all messages received to this connection.
  *                            Valid range: 0 <= sample_rate <= 99 (0 disables sampling)
  *                            Defaults to 0
  * @param msgTimeout          (nsqd v0.2.28+) configure the server-side message timeout in milliseconds for messages
  *                            delivered to this client.
  *
  *
  */
case class NSQConfig(
  clientId: String,
  hostname: String,
  userAgent: String,
  featureNegotiation: Boolean = true,
  heartbeatInterval: Option[Int] = None,
  outputBufferSize: Option[Int] = None,
  outputBufferTimeout: Option[Long] = None,
  tlsV1: Option[Boolean] = None,
  snappy: Option[Boolean] = None,
  deflate: Option[Boolean] = None,
  deflateLevel: Option[Int] = None,
  sampleRate: Option[Int] = None,
  msgTimeout: Option[Long] = None
) {

  import NSQConfig._

  def asJson(out: ByteBuf) = {
    out.writeByte('{')
    out.writeStringField("client_id", clientId)
    out.writeNextStringField("hostname", hostname)
    out.writeNextBooleanField("feature_negotiation", featureNegotiation)
    out.writeNextIntField("heartbeat_interval", heartbeatInterval)
    out.writeNextIntField("output_buffer_size", outputBufferSize)
    out.writeNextLongField("output_buffer_timeout", outputBufferTimeout)
    out.writeNextBooleanField("tls_v1", tlsV1)
    out.writeNextBooleanField("snappy", snappy)
    out.writeNextBooleanField("deflate", deflate)
    out.writeNextIntField("deflate_level", deflateLevel)
    out.writeNextIntField("sample_rate", sampleRate)
    out.writeNextLongField("msg_timeout", msgTimeout)
    out.writeNextStringField("user_agent", userAgent)
    out.writeByte('}')
  }
}

private[nsq] object NSQConfig {

  implicit class ByteBufAscii(buf: ByteBuf) {

    private def writeField(field: String, value: String): Unit = {
      buf.writeByte('"')
      field.foreach(buf.writeByte(_))
      buf.writeByte('"')
      buf.writeByte(':')
      value.foreach(buf.writeByte(_))
    }

    def writeStringField(field: String, value: String): Unit = {
      buf.writeByte('"')
      field.foreach(buf.writeByte(_))
      buf.writeByte('"')
      buf.writeByte(':')
      buf.writeByte('"')
      value.foreach(buf.writeByte(_))
      buf.writeByte('"')
    }

    def writeNextBooleanField(field: String, value: Option[Boolean]): Unit = {
      if (value.isDefined) {
        buf.writeByte(',')
        writeField(field, value.get.toString)
      }
    }

    def writeNextIntField(field: String, value: Option[Int]): Unit = {
      if (value.isDefined) {
        buf.writeByte(',')
        writeField(field, value.get.toString)
      }
    }

    def writeNextLongField(field: String, value: Option[Long]): Unit = {
      if (value.isDefined) {
        buf.writeByte(',')
        writeField(field, value.get.toString)
      }
    }

    def writeNextStringField(field: String, value: String): Unit = {
      buf.writeByte(',')
      writeStringField(field, value)
    }

    def writeNextStringField(field: String, value: Option[String]): Unit = {
      if (value.isDefined) {
        buf.writeByte(',')
        writeStringField(field, value.get.toString)
      }
    }

    def writeNextBooleanField(field: String, value: Boolean): Unit = {
      buf.writeByte(',')
      writeField(field, value.toString)
    }
  }

  val attr = AttributeKey.valueOf[NSQConfig]("nsq-config")

  def default = NSQConfig(
    clientId = InetAddress.getLocalHost.getHostName,
    hostname = InetAddress.getLocalHost.getCanonicalHostName,
    userAgent = "scala-nsq/1.9"
  )
}