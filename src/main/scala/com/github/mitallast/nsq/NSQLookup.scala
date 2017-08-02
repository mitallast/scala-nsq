package com.github.mitallast.nsq

import java.io.InputStream
import java.net.{HttpURLConnection, InetSocketAddress, SocketAddress, URL}

import com.github.mitallast.nsq.protocol.JsonParser
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.io.Source

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

  def lookup(topic: String): List[SocketAddress] = {
    addresses.flatMap { address ⇒
      try {
        val url = new URL(address)
        val lookupUrl = new URL(url.getProtocol, url.getHost, url.getPort, "/lookup?topic=" + topic)

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
              parseResponse(stream)
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
    }.distinct
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
              parseResponse(stream)
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

  private[nsq] def parseResponse(stream: InputStream): List[SocketAddress] = {
    import JsonParser._
    val json = Source.fromInputStream(stream).mkString
    val parser = new JsonParser(json)

    var addresses = List.empty[SocketAddress]

    def parseResponse(): Unit = {
      assert(parser.next() == startObject)
      while (true) {
        parser.next() match {
          case valueString("data") ⇒
            parseData()
          case valueString("producers") ⇒
            parseProducers()
          case valueString(field) ⇒
            parser.skipField()
          case JsonParser.endObject ⇒
            return
          case token ⇒ throw new RuntimeException("unexpected token: " + token)
        }
      }
    }

    def parseData(): Unit = {
      assert(parser.next() == startObject)
      while (true) {
        parser.next() match {
          case valueString("producers") ⇒
            parseProducers()
          case valueString(name) ⇒
            parser.skipField()
          case JsonParser.endObject ⇒
            return
          case token ⇒ throw new RuntimeException("unexpected token: " + token)
        }
      }
    }

    def parseProducers(): Unit = {
      assert(parser.next() == startArray)
      while (true) {
        parser.next() match {
          case JsonParser.startObject ⇒
            parseProducer()
          case JsonParser.endArray ⇒
            return
          case token ⇒ throw new RuntimeException("unexpected token: " + token)
        }
      }
    }

    def parseProducer(): Unit = {
      var hostname = ""
      var broadcast_address = ""
      var tcp_port = 4150

      while (true) {
        parser.next() match {
          case valueString("hostname") ⇒
            parser.next() match {
              case valueString(addr) ⇒
                hostname = addr
              case token ⇒ throw new RuntimeException("unexpected token: " + token)
            }
          case valueString("broadcast_address") ⇒
            parser.next() match {
              case valueString(addr) ⇒
                broadcast_address = addr
              case token ⇒ throw new RuntimeException("unexpected token: " + token)
            }
          case valueString("tcp_port") ⇒
            parser.next() match {
              case valueLong(port) ⇒
                tcp_port = port.toInt
              case token ⇒ throw new RuntimeException("unexpected token: " + token)
            }
          case valueString(name) ⇒
            parser.skipField()
          case JsonParser.endObject ⇒
            if (broadcast_address != "") {
              addresses = addresses ++ List(new InetSocketAddress(broadcast_address, tcp_port))
            } else if (hostname != "") {
              addresses = addresses ++ List(new InetSocketAddress(hostname, tcp_port))
            }
            return
          case token ⇒ throw new RuntimeException("unexpected token: " + token)
        }
      }
    }

    parseResponse()

    addresses
  }
}