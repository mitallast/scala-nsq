package com.github.mitallast.nsq

object NSQError {
  val E_INVALID = "E_INVALID"
  val E_BAD_BODY = "E_BAD_BODY"
  val E_BAD_MESSAGE = "E_BAD_MESSAGE"
  val E_BAD_TOPIC = "E_BAD_TOPIC"
  val E_BAD_CHANNEL = "E_BAD_CHANNEL"
  val E_PUB_FAILED = "E_PUB_FAILED"
  val E_MPUB_FAILED = "E_MPUB_FAILED"
  val E_FIN_FAILED = "E_FIN_FAILED"
  val E_REQ_FAILED = "E_REQ_FAILED"
  val E_TOUCH_FAILED = "E_TOUCH_FAILED"
  val E_AUTH_FAILED = "E_AUTH_FAILED"
  val E_UNAUTHORIZED = "E_UNAUTHORIZED"
}

sealed class NSQError(message: String) extends Exception(message)

case class NSQErrorInvalid(message: String) extends NSQError(message)
case class NSQErrorBadBody(message: String) extends NSQError(message)
case class NSQErrorBadMessage(message: String) extends NSQError(message)
case class NSQErrorBadTopic(message: String) extends NSQError(message)
case class NSQErrorBadChannel(message: String) extends NSQError(message)
case class NSQErrorPubFailed(message: String) extends NSQError(message)
case class NSQErrorMpubFailed(message: String) extends NSQError(message)
case class NSQErrorFinFailed(message: String) extends NSQError(message)
case class NSQErrorReqFailed(message: String) extends NSQError(message)
case class NSQErrorTouchFailed(message: String) extends NSQError(message)
case class NSQErrorAuthFailed(message: String) extends NSQError(message)
case class NSQErrorUnauthorized(message: String) extends NSQError(message)
case class NSQProtocolError(message: String) extends NSQError(message)
case class NSQDisconnected(message: String) extends NSQError(message)
