package org.mitallast.nsq

class NSQException(message: String) extends Exception(message)
class NoConnectionException(message: String) extends NSQException(message)
class DisconnectedException(message: String) extends NSQException(message)
class BadTopicException(message: String) extends NSQException(message)
class BadMessageException(message: String) extends NSQException(message)
class ProtocolException(message: String) extends NSQException(message)
