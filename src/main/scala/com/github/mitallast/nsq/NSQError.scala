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

class NSQErrorInvalid() extends NSQError(NSQError.E_INVALID)
class NSQErrorBadBody() extends NSQError(NSQError.E_BAD_BODY)
class NSQErrorBadMessage() extends NSQError(NSQError.E_BAD_MESSAGE)
class NSQErrorBadTopic() extends NSQError(NSQError.E_BAD_TOPIC)
class NSQErrorBadChannel() extends NSQError(NSQError.E_BAD_CHANNEL)
class NSQErrorPubFailed() extends NSQError(NSQError.E_PUB_FAILED)
class NSQErrorMpubFailed() extends NSQError(NSQError.E_MPUB_FAILED)
class NSQErrorFinFailed() extends NSQError(NSQError.E_FIN_FAILED)
class NSQErrorReqFailed() extends NSQError(NSQError.E_REQ_FAILED)
class NSQErrorTouchFailed() extends NSQError(NSQError.E_TOUCH_FAILED)
class NSQErrorAuthFailed() extends NSQError(NSQError.E_AUTH_FAILED)
class NSQErrorUnauthorized() extends NSQError(NSQError.E_UNAUTHORIZED)

class NSQProtocolError(message: String) extends NSQError(message)

class NSQDisconnected(message: String) extends NSQError(message)
