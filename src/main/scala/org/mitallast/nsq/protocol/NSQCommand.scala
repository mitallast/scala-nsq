package org.mitallast.nsq.protocol

private [nsq] sealed trait NSQCommand

/**
  * Update client metadata on the server and negotiate features
  *
  * Success Response:
  * - OK
  *
  * NOTE: if feature_negotiation was sent by the client (and the server supports it) the response will be a JSON payload as described above.
  *
  * Error Responses:
  * - E_INVALID
  * - E_BAD_BODY
  *
  * @param config see NSQConfig for more details
  */
private [nsq] case class IdentifyCommand(config: NSQConfig) extends NSQCommand

/**
  * Subscribe to a topic/channel
  *
  * Success response:
  * - OK
  *
  * Error Responses:
  * - E_INVALID
  * - E_BAD_TOPIC
  * - E_BAD_CHANNEL
  *
  * @param topic   a valid string (optionally having #ephemeral suffix)
  * @param channel a valid string (optionally having #ephemeral suffix)
  */
private [nsq] case class SubCommand(topic: String, channel: String = "default") extends NSQCommand

/**
  * Publish a message to a topic:
  *
  * Success Response:
  * - OK
  *
  * Error Responses:
  * - E_INVALID
  * - E_BAD_TOPIC
  * - E_BAD_MESSAGE
  * - E_PUB_FAILED
  *
  * @param topic a valid string (optionally having #ephemeral suffix)
  * @param data  N-byte binary data
  */
private [nsq] case class PubCommand(topic: String, data: Array[Byte]) extends NSQCommand

/**
  * Publish multiple messages to a topic (atomically):
  *
  * Success Response:
  * - OK
  *
  * Error Responses:
  * - E_INVALID
  * - E_BAD_TOPIC
  * - E_BAD_BODY
  * - E_BAD_MESSAGE
  * - E_MPUB_FAILED
  *
  * @param topic a valid string (optionally having #ephemeral suffix)
  * @param data  N-byte binary data
  */
private [nsq] case class MPubCommand(topic: String, data: Seq[Array[Byte]]) extends NSQCommand

/**
  * Update RDY state (indicate you are ready to receive N messages)
  *
  * NOTE: as of nsqd v0.2.20+ use --max-rdy-count to bound this value
  * NOTE: there is no success response
  *
  * Error Responses:
  * - E_INVALID
  *
  * @param count 0 < count <= configured_max
  */
private [nsq] case class RdyCommand(count: Int) extends NSQCommand

/**
  * Finish a message (indicate successful processing)
  *
  * NOTE: there is no success response
  *
  * Error Responses:
  * - E_INVALID
  * - E_FIN_FAILED
  *
  * @param messageId message id as 16-byte hex string
  */
private [nsq] case class FinCommand(messageId: String) extends NSQCommand

/**
  * Re-queue a message (indicate failure to process)
  * The re-queued message is placed at the tail of the queue, equivalent to having just published it, but for various
  * implementation specific reasons that behavior should not be explicitly relied upon and may change in the future.
  * Similarly, a message that is in-flight and times out behaves identically to an explicit REQ.
  *
  * NOTE: there is no success response
  *
  * Error Responses:
  * - E_INVALID
  * - E_REQ_FAILED
  *
  * @param messageId message id as 16-byte hex string
  * @param timeout   a string representation of integer N where N <= configured max timeout
  *                  0 is a special case that will not defer re-queueing
  */
private [nsq] case class ReqCommand(messageId: String, timeout: Int) extends NSQCommand

/**
  * Reset the timeout for an in-flight message
  * NOTE: available in nsqd v0.2.17+
  *
  * NOTE: there is no success response
  *
  * Error Responses:
  * - E_INVALID
  * - E_TOUCH_FAILED
  *
  * @param messageId the hex id of the message
  */
private [nsq] case class TouchCommand(messageId: String) extends NSQCommand

/**
  * Cleanly close your connection (no more messages are sent)
  *
  * Success Responses:
  * - CLOSE_WAIT
  *
  * Error Responses:
  * - E_INVALID
  */
private [nsq] case object ClsCommand extends NSQCommand

/**
  * No-op
  *
  * NOTE: there is no response
  */
private [nsq] case object NopCommand extends NSQCommand

/**
  * NOTE: available in nsqd v0.2.29+
  *
  * If the IDENTIFY response indicates auth_required=true the client must send AUTH before any SUB, PUB or MPUB
  * commands. If auth_required is not present (or false), a client must not authorize.
  *
  * When nsqd receives an AUTH command it delegates responsibility to the configured --auth-http-address by performing
  * an HTTP request with client metadata in the form of query parameters: the connection’s remote address, TLS state,
  * and the supplied auth secret. See AUTH for more details.
  *
  * Success Response:
  * - A JSON payload describing the authorized client’s identity, an optional URL and a count of permissions which
  * were authorized.
  * <code>
  * {"identity":"...", "identity_url":"...", "permission_count":1}
  * </code>
  *
  * Error Responses:
  * - E_AUTH_FAILED - An error occurred contacting an auth server
  * - E_UNAUTHORIZED - No permissions found
  *
  * @param secret N-byte Auth Secret
  */
private [nsq] case class AuthCommand(secret: Array[Byte]) extends NSQCommand