package com.github.mitallast.nsq.protocol

object JsonParser {
  sealed trait token
  case object start extends token
  case object startObject extends token
  case object endObject extends token
  case object startArray extends token
  case object endArray extends token
  case class valueString(name: String) extends token
  case class valueLong(name: Long) extends token
  case class valueBoolean(name: Boolean) extends token
}

class JsonParser(input: String) {

  import JsonParser._

  private val iterator = input.iterator

  private var last: token = start

  def next(): token = {
    last = iterator.next() match {
      case '{' ⇒ startObject
      case '[' ⇒ startArray
      case ']' ⇒ endArray
      case '}' ⇒ endObject
      case ':' ⇒ next()
      case ',' ⇒ next()
      case ' ' ⇒ next()
      case '"' ⇒
        val name = iterator.takeWhile(_ != '"').mkString
        valueString(name)
      case 't' ⇒
        assert(iterator.next() == 'r')
        assert(iterator.next() == 'u')
        assert(iterator.next() == 'e')
        valueBoolean(true)
      case 'f' ⇒
        assert(iterator.next() == 'a')
        assert(iterator.next() == 'l')
        assert(iterator.next() == 's')
        assert(iterator.next() == 'e')
        valueBoolean(false)
      case d if d.isDigit ⇒
        val value = d + iterator.takeWhile(_.isDigit).mkString
        valueLong(value.toInt)
    }
    last
  }

  def skipField(): Unit = {
    next() match {
      case JsonParser.startObject ⇒
        skipObject()
      case JsonParser.startArray ⇒
        skipArray()
      case _: valueString ⇒
      case _: valueLong ⇒
      case _: valueBoolean ⇒
      case token ⇒ throw new RuntimeException("unexpected token: " + token)
    }
  }

  def skipObject(): Unit = {
    while (true) {
      next() match {
        case _: valueString ⇒ skipField()
        case JsonParser.endObject ⇒
          return
        case token ⇒ throw new RuntimeException("unexpected token: " + token)
      }
    }
  }

  def skipArray(): Unit = {
    while (true) {
      next() match {
        case JsonParser.startObject ⇒ skipObject()
        case JsonParser.startArray ⇒ skipArray()
        case _: valueString ⇒ // ignore
        case _: valueLong ⇒ // ignore
        case _: valueBoolean ⇒ // ignore
        case JsonParser.endArray ⇒
          return
        case token ⇒ throw new RuntimeException("unexpected token: " + token)
      }
    }
  }
}
