package com.github.mitallast.nsq

import java.net.{HttpURLConnection, InetSocketAddress, SocketAddress, URL}

import com.typesafe.config.{Config, ConfigFactory}
import org.json4s.{DefaultFormats, StreamInput}
import org.json4s.jackson.JsonMethods._
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

trait NSQLookup {

  def lookup(topic: String): List[SocketAddress]

  def nodes(): List[SocketAddress]
}

object NSQLookup {

  def apply(): NSQLookup = apply(ConfigFactory.load())

  def apply(config: Config): NSQLookup = apply(config.withFallback(ConfigFactory.defaultReference()).getStringList("nsq.lookup-address").toList)

  def apply(address: String): NSQLookup = apply(List(address))

  def apply(addresses: List[String]): NSQLookup = new NSQLookupDefault(addresses)
}

class NSQLookupDefault(addresses: List[String]) extends NSQLookup {
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val formats = DefaultFormats

  def lookup(topic: String): List[SocketAddress] = {
    addresses.flatMap { address ⇒
      try {
        val url = new URL(address)
        val lookupUrl = new URL(url.getProtocol, url.getHost, url.getPort, "/lookup/?topic=" + topic)

        if (log.isDebugEnabled) {
          log.debug(s"lookup $url topic $topic")
        }
        val connection = lookupUrl.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.connect()
        connection.getResponseCode match {
          case 200 ⇒
            val stream = connection.getInputStream
            try {
              val json = parse(StreamInput(stream)).camelizeKeys
              val response = json.extract[LookupResponse]
              if (log.isDebugEnabled) {
                log.debug("response: {}", response)
              }

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

  def nodes(): List[SocketAddress] = {
    addresses.flatMap { address ⇒
      try {

        val url = new URL(address)
        val lookupUrl = new URL(url.getProtocol, url.getHost, url.getPort, "/nodes")

        if (log.isDebugEnabled) {
          log.debug(s"lookup $url nodes")
        }
        val connection = lookupUrl.openConnection().asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("GET")
        connection.connect()
        connection.getResponseCode match {
          case 200 ⇒
            val stream = connection.getInputStream
            try {
              val json = parse(StreamInput(stream)).camelizeKeys
              val response = json.extract[LookupResponse]
              if (log.isDebugEnabled) {
                log.debug("response: {}", response)
              }
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