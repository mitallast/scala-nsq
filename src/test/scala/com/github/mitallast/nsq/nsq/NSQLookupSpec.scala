package com.github.mitallast.nsq.nsq

import java.net.InetSocketAddress

import org.scalatest.{FlatSpec, Matchers}

class NSQLookupSpec extends FlatSpec with Matchers {

  "nsq lookup" should "lookup topics" in {
    val lookup = new NSQLookup(List("http://127.0.0.1:4161"))
    val address = lookup.lookup("scala.nsq.test")
    address.nonEmpty shouldBe true
    address.head.asInstanceOf[InetSocketAddress].getPort shouldBe 4150
  }

  it should "lookup nodes" in {
    val lookup = new NSQLookup(List("http://127.0.0.1:4161"))
    val address = lookup.nodes()
    address.nonEmpty shouldBe true
    address.head.asInstanceOf[InetSocketAddress].getPort shouldBe 4150
  }
}
