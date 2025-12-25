import pika
import pika.frame as pika_frame
import threading
from logging import Logger
from pika import SelectConnection
from pika.channel import Channel
from pika.connection import Connection
from pika.exchange_type import ExchangeType
from pika.spec import BasicProperties
from pypomes_core import Mimetype
from typing import Any

from .mq_config import MqState


class _MqPublisher(threading.Thread):
    """
    A wrapper on *pika*, an implementation of the publisher side of a *RabbitMQ* client.

    This is an example of a publisher that can handle unexpected interactions with *RabbitMQ*,
    such as channel closing and connection interruptions.

    If *RabbitMQ* closes the connection, this implementation will attempt to reopen it.
    In these case, the execution log should be examined, as there are only a few reasons
     to justify the closing of the connection, and they are usually related to permissions or
    time limitations on the use of sockets.

    This implementation uses delivery confirmation, by providing a means to follow up
    on message delivery, and verify if such deliveries were confirme by *RabbitMQ*.
    """
    # the class logger
    LOGGER: Logger = None

    def __init__(self,
                 mq_url: str,
                 exchange_name: str,
                 exchange_type: str,
                 max_reconnect_delay: int) -> None:
        """
        Create a new instance of the publisher, with the arguments to allow it to interact with *RabbitMQ*.

        :param mq_url: A URL usada for the connection
        :param exchange_name: name of the exchange to use
        :param exchange_type: type of the exchange
        :param max_reconnect_delay: maximum delay for re-establishing lost connections, in soeconds
        """
        threading.Thread.__init__(self)

        # initialize instance attributes
        self.exchange_name: str = exchange_name

        self.exchange_type: str
        match exchange_type:
            case "direct":
                self.exchange_type = str(ExchangeType.direct.value)
            case "fanout":
                self.exchange_type = str(ExchangeType.fanout.value)
            case "headers":
                self.exchange_type = str(ExchangeType.headers.value)
            case _:  # 'topic'
                self.exchange_type = str(ExchangeType.topic.value)

        self.started_publishing: bool = False
        self.mq_url: str = mq_url
        self.stopped: bool = False
        self.acked: int = 0
        self.nacked: int = 0
        self.msg_last_filed: int = 0
        self.msg_last_sent: int = 0
        self.publish_interval: int = 1

        self.reconnect_delay: int = 0
        self.max_reconnect_delay: int = max_reconnect_delay

        self.conn: SelectConnection | None = None
        self.channel: Channel | None = None

        self.state: int = MqState.INITIALIZING
        self.state_msg: str = "Attenpting to instantiate the publisher"

        # structure ('n' is the sequential message number, int > 0):
        # <{ n: { "header": <str>,
        #        "body":  <bytes>,
        #        "mimetype": <str>,
        #        "routing_key": <str>
        #      },...
        # }>
        self.messages: dict | None = None

        self.messages = None
        # mutex for controlling concurrent access to the message structure
        self.msg_lock: threading.Lock = threading.Lock()

        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg="Publisher instantiated, with exchange "
                                          f"'{exchange_name}' of type '{exchange_type}'")

    # ponto de entrada para a thread
    def run(self) -> None:
        """
        Initialize the publisher, by connecting with *RabbitMQ* e initiating the *IOLoop*.
        """
        # stay in the loop, until 'stop()' is invoked
        while not self.stopped:
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug(msg="Started")
            self.messages = {}
            self.acked = 0
            self.nacked = 0
            self.msg_last_sent = 0

            # conect with RabbitMQ
            self.conn = self.connect()

            # initiate the IOLoop, blocking until it is interrupted
            self.conn.ioloop.start()

        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug("Finished")

    def connect(self) -> SelectConnection:
        """
        Connect with *RabbitMQ*, and return the connection identifier.

        When the connection is established, *on_connection_open* will be invooked by *pika*.

        :return: the connection obtained
        """
        # do not write user and password from URL in the log
        #   url: <protocol>//<user>:<password>@<ip-address>
        if _MqPublisher.LOGGER:
            first: int = self.mq_url.find("//")
            last = self.mq_url.find("@")
            _MqPublisher.LOGGER.debug(msg=f"Connecting with "
                                          f"'{self.mq_url[0:first]}{self.mq_url[last:]}'")

        # obtain anf return the connection
        return SelectConnection(
            pika.URLParameters(self.mq_url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self,
                           _connection: Connection) -> None:
        """
        Account for *pika*'s *callback* invocation, when the connection with *RabbitMQ* is established.

        The identifier for the object is given, to be used as needed. At the moment, it is marked as not used.

        :param _connection: the connection with RabbitMQ
        """
        self.state = MqState.CONNECTION_OPEN
        self.state_msg = "Connection was open"
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(self.state_msg)
        self.open_channel()

    def on_connection_open_error(self,
                                 _connection: Connection,
                                 error: Exception) -> None:
        """
         Account for *pika*'s *callback* invocation, if the connection with *RabbitMQ* could not be established.

        :param _connection: the attempted connection RabbitMQ
        :param error: the corresponding error message
        """
        self.state = MqState.CONNECTION_ERROR
        self.state_msg = f"Error establishing connection: {error}"
        delay: int = self.__get_reconnect_delay()
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.error(self.state_msg)
            _MqPublisher.LOGGER.debug(msg=f"Reconnecting in {delay} seconds")
        self.conn.ioloop.call_later(delay=delay,
                                    callback=self.conn.ioloop.stop)

    def on_connection_closed(self,
                             _connection: Connection,
                             reason: Exception) -> None:
        """
        Account for *pika*'s *callback* invocation, when the connection with *RabbitMQ* is closed unexpectedly.

        In this situation, reconnecting with *RabbitMQ* is attempted.

        :param _connection: the closed connection
        :param reason: exception indicating the reason for the connection loss
        """
        self.state = MqState.CONNECTION_CLOSED
        self.state_msg = f"Connection was closed: {reason}"
        self.channel = None
        if self.stopped:
            self.conn.ioloop.stop()
        else:
            delay: int = self.__get_reconnect_delay()
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.warning(msg=self.state_msg)
                _MqPublisher.LOGGER.debug(msg=f"Reconnecting in {delay} seconds")
            self.conn.ioloop.call_later(delay=delay,
                                        callback=self.conn.ioloop.stop)

    def open_channel(self) -> None:
        """
        Open a new channel with *RabbitMQ*, by means of the RPC *Channel.Open* command.

        When the channel open response from *RabbitMQ* is received, the indicated *callback* is invoked by *pika*.
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg="Creating a new channel")
        self.conn.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self,
                        channel: Channel) -> None:
        """
         Account for *pika*'s *callback* invocation, when the channel is open.

         The channel object isgiven as parameter, to be used as needed. With the channel now open,
         the exchange to use is declared.

        :param channel: O canal que foi aberto
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg="The channel is open, "
                                          "establishing the callback for channel closing")
        self.channel = channel
        self.channel.add_on_close_callback(callback=self.on_channel_closed)
        self.setup_exchange()

    def on_channel_closed(self,
                          channel: Channel,
                          reason: Exception) -> None:
        """
        Account for *pika*'s *callback* invocation, when *RabbitMQ* unexpectedly close the channel.

        Channels are usually closed when a protocol violation is attempted,
        such as redeclaring the exchange or queue with different parameters.
        In this situation, the connection is also closed, to allow for the object *shutdown*.

        :param channel: the closed channel
        :param reason: the reason for closing the channel
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.warning(msg=f"The channel '{channel}' has been closed: {reason}")
        self.channel = None
        if not self.stopped and not self.conn.is_closed and not self.conn.is_closing:
            self.conn.close()

    def setup_exchange(self) -> None:
        """
        Verify if the exchange is properly configured in *RabbitMQ*.

        This is done with theRPC *Exchange.Declare* command, with the parameter *passive=True*.
         If this configuraation is confirmed, *on_exchange_declare_ok* will be invoked by *pika*.
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg=f"Declaring the exchange '{self.exchange_name}'")
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      passive=True,
                                      durable=True,
                                      callback=self.on_exchange_declare_ok)

    def on_exchange_declare_ok(self,
                               _unused_frame: pika_frame.Method) -> None:
        """
         Account for *pika*'s *callback* invocation, when *RabbitMQ* concludes the RPC *Exchange.Declare*.

        Enable delivery confirmations and schedule the first message to be sent to *RabbitMQ*.
        Send the RPC command *confirm_delivery* to *RabbitMQ* to enable delivery of confirmations on the channel.

        The only way to disable this is to close the channel and create a new one. When the message from *RabitMQ*
        is confirmed, the *on_delivery_confirmation* method will be invoked, passing a *Basic.Ack* or *Basic.Nack*.
        This will indicate which messages are being confirmed or rejected.

        :param _unused_frame: Exchange.DeclareOk response frame
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg=f"Exchange '{self.exchange_name}' "
                                          f"declared, ready for publishing")
        self.channel.confirm_delivery(ack_nack_callback=self.on_delivery_confirmation)
        self.started_publishing = True

    def on_delivery_confirmation(self,
                                 method_frame: pika_frame.Method) -> None:
        """
        Account for *pika*'s *callback* invocation, when *RabbitMQ* responds to a RPC *Basic.Publish* command.

        This is done with passing a *frame* *Basic.Ack* or *Basic.Nack*,
        with the delivery tag of the published message.

        The delivery tag is an integer counter, indicating the sequencial number of the message,
        sent in the channel via *Basic.Publish*. Maintenance of the message structure used for
        managing messaages, to be sent or still pending confirmation, is carried out.
        Statistics are logged.

        :param method_frame: frame Basic.Ack ou Basic.Nack
        """
        confirmation_type: str = method_frame.method.NAME.split(".")[1].lower()
        ack_multiple: bool = method_frame.method.multiple
        delivery_tag: int = method_frame.method.delivery_tag

        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug(msg=(f"Delivery confirmation received: tag '{delivery_tag}', "
                                           f"type '{confirmation_type}', multiple: {ack_multiple}"))

        if confirmation_type == "ack":
            self.acked += 1
        else:  # elif confirmation_type == "nack":
            self.nacked += 1

        with self.msg_lock:
            self.messages.pop(delivery_tag)

            if ack_multiple:
                msg_tags: list[int] = []
                for msg_tag in self.messages:
                    if msg_tag <= delivery_tag:
                        msg_tags.append(msg_tag)
                        if confirmation_type == "ack":
                            self.acked += 1
                        else:  # confirmation_type == "nack":
                            self.nacked += 1

                for msg_tag in msg_tags:
                    self.messages.pop(msg_tag)

            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug(msg=f"Messages: published {self.msg_last_sent}, "
                                              f"to be confirmed {len(self.messages)}, "
                                              f"successful {self.acked}, unsuccessful {self.nacked}")

    def send_message(self) -> None:
        """
        Publish a message in *RabbitMQ*, unless the publisher is stopping.

        This is done by adding the message data to the *messages* object. This object is used to verify
        the delivery confirmation in *on_delivery_confirmation*.
        """
        # is there an open channel ?
        if self.channel is not None and self.channel.is_open:
            # yes, proceed
            self.msg_last_sent += 1

            with self.msg_lock:
                message: dict[str, Any] = self.messages[self.msg_last_sent]

            properties: BasicProperties = pika.BasicProperties(app_id="mq-publisher",
                                                               content_type=message.get("mimetype"),
                                                               headers=message.get("headers"))
            routing_key: str = message.get("routing_key")

            msg_body: bytes = message.get("body")
            self.channel.basic_publish(exchange=self.exchange_name,
                                       routing_key=routing_key,
                                       body=msg_body,
                                       properties=properties)
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug(msg=f"Msg '{self.msg_last_sent}' "
                                              f"published, key '{routing_key}'")
        elif _MqPublisher.LOGGER:
            # no, report the error
            _MqPublisher.LOGGER.error(msg="Publishing is not possible: "
                                          "no open channel with the server")

    def publish_message(self,
                        msg_body: str | bytes,
                        routing_key: str,
                        msg_mimetype: Mimetype = Mimetype.TEXT,
                        msg_headers: str = None,
                        errors: list[str] = None) -> None:
        """
        Publish a message in *RabbitMQ*, unless the publisher is stopping.

        This is the interface for external requests for message publishing.
        *RabbitMQ* is told to invoke *send_message* in *publish_interval* seconds.
        The delivery intervals may be accelerated or decelerated, by changing this variable.

        :param msg_body: body of the message
        :param routing_key: key for message routing
        :param msg_mimetype: message mimetype (defaults to type text)
        :param msg_headers: optional message headers
        :param errors: incidental errors (might be a non-empty list)
        """
        # is there an open channel ?
        if self.channel is not None and self.channel.is_open:
            # yes, proceed
            msg_bytes: bytes = msg_body if isinstance(msg_body, bytes) else msg_body.encode()
            self.msg_last_filed += 1

            with self.msg_lock:
                self.messages[self.msg_last_filed] = {
                    "headers": msg_headers,
                    "body": msg_bytes,
                    "mimetype": msg_mimetype,
                    "routing_key": routing_key
                }

            # schedule message delivery to happen in 'publish_interval' seconds
            self.conn.ioloop.call_later(delay=self.publish_interval,
                                        callback=self.send_message)
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug(msg=f"Msg '{self.msg_last_filed}' scheduled for publication in "
                                              f"{self.publish_interval} secs,"
                                              f" routing key '{routing_key}': {msg_bytes.decode()}")
        else:
            # no, report the error
            errmsg: str = "Messages refused: no open channel to the server exists"
            if isinstance(errors, list):
                errors.append(errmsg)
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.error(msg=errmsg)

    def get_state(self) -> int:
        """
        Return the current state of the events publisher.

         This should be one of:
            - MQP_CONNECTION_OPEN
            - MQP_CONNECTION_CLOSED
            - MQP_CONNECTION_ERROR
            - MQP_INITIALIZING

        :return: the current state of the publisher
        """
        return self.state

    def get_state_msg(self) -> str:
        """
        Return the message associated with the current state of the publisher.

        :return: the current state message.
        """

    def stop(self) -> None:
        """
        Stop the publisher, by closing the channel and the connection.

        A flag is turned on, to signal the need for interrupting the scheduling of new messages publication.
        """
        if _MqPublisher.LOGGER:
            _MqPublisher.LOGGER.debug("Finishing...")
        self.stopped = True
        self.close_channel()
        self.close_connection()

    def close_channel(self) -> None:
        """
        Close channel with *RabbitMQ*, by sending the RPC command *Channel.Close*.
        """
        if self.channel:
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug("Closing the channel...")
            self.channel.close()

    def close_connection(self) -> None:
        """
        Close the connection with *RabbitMQ*.
        """
        if self.conn:
            if _MqPublisher.LOGGER:
                _MqPublisher.LOGGER.debug("Closing the connection...")
            self.conn.close()

    def __get_reconnect_delay(self) -> int:
        """
        Update and return the value of the reconnection delay.

        This value is incremented by 1 every time it is retrieved, until the maximum value is reached.

        :return: the reconnection delay, in seconds.
        """
        if self.started_publishing:
            self.reconnect_delay = 0
        else:
            self.reconnect_delay += 1

        self.reconnect_delay = max(self.reconnect_delay, self.max_reconnect_delay)

        return self.reconnect_delay

    @staticmethod
    def set_logger(logger: Logger) -> None:
        """
        Establish the class logger.

        :param logger: the class logger
        """
        _MqPublisher.LOGGER = logger
