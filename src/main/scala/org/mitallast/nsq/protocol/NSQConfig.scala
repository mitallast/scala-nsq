package org.mitallast.nsq.protocol

import java.net.InetAddress

import io.netty.buffer.ByteBuf
import io.netty.util.AttributeKey

/**
  *
  * @param clientId            an identifier used to disambiguate this client (ie. something specific to the consumer)
  * @param hostname            the hostname where the client is deployed
  * @param userAgent           (nsqd v0.2.25+) a string identifying the agent for this client in the spirit of HTTP
  *                            Default: <client_library_name>/<version>
  * @param compression         snappy (nsqd v0.2.23+) enable snappy compression for this connection.
  *                            --snappy (nsqd flag) enables support for this server side
  *
  *                            deflate (nsqd v0.2.23+) enable deflate compression for this connection.
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
  compression: String = "NONE",
  featureNegotiation: Boolean = true,
  heartbeatInterval: Option[Int] = None,
  outputBufferSize: Option[Int] = None,
  outputBufferTimeout: Option[Int] = None,
  tlsV1: Option[Boolean] = None,
  deflateLevel: Option[Int] = None,
  sampleRate: Option[Int] = None,
  msgTimeout: Option[Int] = None
) {

  def toJson(out: ByteBuf) = {
    val json = JsonBuf(out)
    json.field("client_id").value(clientId)
    json.field("hostname").value(hostname)
    json.field("feature_negotiation").value(featureNegotiation)
    json.putInt("heartbeat_interval", heartbeatInterval)
    json.putInt("output_buffer_size", outputBufferSize)
    json.putInt("output_buffer_timeout", outputBufferTimeout)
    json.putBoolean("tls_v1", tlsV1)
    compression match {
      case "SNAPPY" ⇒ json.field("snappy").value(true)
      case "DEFLATE" ⇒ json.field("snappy").value(true)
      case _ ⇒ // ignore
    }
    json.putInt("deflate_level", deflateLevel)
    json.putInt("sample_rate", sampleRate)
    json.putInt("msg_timeout", msgTimeout)
    json.field("user_agent").value(userAgent)
    json.close
  }
}

private [nsq] object NSQConfig {

  val attr = AttributeKey.valueOf[NSQConfig]("nsq-config")

  def default = NSQConfig(
    clientId = InetAddress.getLocalHost.getHostName,
    hostname = InetAddress.getLocalHost.getCanonicalHostName,
    userAgent = "scala-nsq-client/1.0"
  )
}