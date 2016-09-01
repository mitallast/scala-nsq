package org.mitallast.nsq.protocol

import io.netty.buffer.ByteBuf

private [nsq] case class JsonBuf(out: ByteBuf) {
  out.writeByte('{')

  private var next = false

  def close = {
    out.writeByte('}')
  }

  def value(value: String) = {
    out.writeByte('"')
    value.foreach(out.writeByte(_))
    out.writeByte('"')
    this
  }

  def value(value: Boolean) = {
    value.toString.foreach(out.writeByte(_))
    this
  }

  def value(value: Int) = {
    value.toString.foreach(out.writeByte(_))
    this
  }

  def field(name: String) = {
    if (next) {
      out.writeByte(',')
    }
    out.writeByte('"')
    name.foreach(out.writeByte(_))
    out.writeByte('"')
    out.writeByte(':')
    next = true
    this
  }

  def putString(name: String, value: Option[String]) = {
    value.foreach(value ⇒ field(name).value(value))
    this
  }

  def putBoolean(name: String, value: Option[Boolean]) = {
    value.foreach(value ⇒ field(name).value(value))
    this
  }

  def putInt(name: String, value: Option[Int]) = {
    value.foreach(value ⇒ field(name).value(value))
    this
  }
}