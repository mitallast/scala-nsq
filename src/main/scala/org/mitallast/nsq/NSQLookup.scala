package org.mitallast.nsq

import java.net.{HttpURLConnection, InetSocketAddress, URL}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

class NSQLookup(addresses: List[String]) {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val formats = DefaultFormats

  def lookup(topic: String): List[InetSocketAddress] = {
    addresses.flatMap { address ⇒
      try {
        val url = new URL(address)
        val lookupUrl = new URL(url.getProtocol, url.getHost, url.getPort, "/lookup/?topic=" + topic)

        log.info(s"lookup $url topic $topic")
        val connection = lookupUrl.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.connect()
        connection.getResponseCode match {
          case 200 ⇒
            val stream = connection.getInputStream
            try {
              val json = parse(StreamInput(stream)).camelizeKeys
              val response = json.extract[LookupResponse]
              log.info("response: {}", response)

              response.data.producers.map { producer ⇒
                new InetSocketAddress(producer.hostname, producer.tcpPort)
              }
            } finally {
              stream.close()
            }
          case _ ⇒
            List.empty
        }

      } catch {
        case e: Exception ⇒
          log.warn(s"failed lookup $address", e)
          List.empty
      }
    }
  }

  def nodes(): List[InetSocketAddress] = {
    addresses.flatMap { address ⇒
      try {

        val url = new URL(address)
        val lookupUrl = new URL(url.getProtocol, url.getHost, url.getPort, "/nodes")

        log.info(s"lookup $url nodes")
        val connection = lookupUrl.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.connect()
        connection.getResponseCode match {
          case 200 ⇒
            val stream = connection.getInputStream
            try {
              val json = parse(StreamInput(stream)).camelizeKeys
              val response = json.extract[LookupResponse]
              log.info("response: {}", response)
              response.data.producers.map { producer ⇒
                new InetSocketAddress(producer.hostname, producer.tcpPort)
              }
            } finally {
              stream.close()
            }
          case _ ⇒
            List.empty
        }
      } catch {
        case e: Exception ⇒
          log.warn(s"failed lookup nodes", e)
          List.empty
      }
    }
  }

  private[nsq] case class LookupResponse(statusCode: Int, statusTxt: String, data: LookupData)
  private[nsq] case class LookupData(channels: List[String], producers: List[LookupProducer])
  private[nsq] case class LookupProducer(
    remoteAddress: String,
    hostname: String,
    broadcastAddress: String,
    tcpPort: Int,
    httpPort: Int,
    version: String,
    tombstones: List[Boolean],
    topics: List[String]
  )
}
