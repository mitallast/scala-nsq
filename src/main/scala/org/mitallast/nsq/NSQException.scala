package org.mitallast.nsq

object NSQError {
  val E_INVALID = "E_INVALID"
  val E_BAD_BODY = "E_BAD_BODY"
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

class NSQException(message: String) extends Exception(message)

class NSQErrorInvalid() extends NSQException(NSQError.E_INVALID)
class NSQErrorBadBody() extends NSQException(NSQError.E_BAD_BODY)
class NSQErrorBadTopic() extends NSQException(NSQError.E_BAD_TOPIC)
class NSQErrorBadChannel() extends NSQException(NSQError.E_BAD_CHANNEL)
class NSQErrorPubFailed() extends NSQException(NSQError.E_PUB_FAILED)
class NSQErrorMpubFailed() extends NSQException(NSQError.E_MPUB_FAILED)
class NSQErrorFinFailed() extends NSQException(NSQError.E_FIN_FAILED)
class NSQErrorReqFailed() extends NSQException(NSQError.E_REQ_FAILED)
class NSQErrorTouchFailed() extends NSQException(NSQError.E_TOUCH_FAILED)
class NSQErrorAuthFailed() extends NSQException(NSQError.E_AUTH_FAILED)
class NSQErrorUnauthorized() extends NSQException(NSQError.E_UNAUTHORIZED)

class NSQProtocolException(message: String) extends NSQException(message)

class NSQNoConnectionException(message: String) extends NSQException(message)
class NSQDisconnectedException(message: String) extends NSQException(message)
