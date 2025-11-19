import time
import threading
from collections.abc import Callable
from logging import Logger
from pika import frame as pika_frame
from pika import spec as pika_spec
from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.connection import Connection
from pika.exchange_type import ExchangeType

from .mq_config import MqState


class _MqSubscriber:
    """
    A wrapper on *pika*, an implementation of the subscriber side of a *RabbitMQ* client.

    This is an example of a consumer that can handle unexpected interactions with *RabbitMQ*,
    such as channel closing and connection interruptions.

    If *RabbitMQ* closes the connection, this object will stop and indicate that reconnecting is required.
    In this case, the execution log should be examined, as the reasons why the connection can be
    closed are limited, and are often tied to permissions-related issues, or to socket timeouts.
    If the channel is closed, it indicates a problem with one of the commands that were issued.
    """

    def __init__(self,
                 mq_url: str,
                 exchange_name: str,
                 exchange_type: str,
                 queue_name: str,
                 msg_target: Callable,
                 logger: Logger = None) -> None:
        """
        Create an instance of the consumer, witth the arguments needed for interacting with *RabbitMQ*.

        :param mq_url: URL used in the connection
        :param exchange_name: name of the exchange to use
        :param exchange_type: type of the exchange
        :param queue_name: name of the message queue to use
        :param msg_target: callback for message deliveries
        :param logger: optional logger
        """
        # initialize instance attributes
        self.exchange_name = exchange_name

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

        self.should_reconnect: bool = False
        self.started_consumption: bool = False
        self.logger: Logger = logger
        self.closing: bool = False
        self.consuming: bool = False
        self.consumer_tag: str | None = None
        self.mq_url: str = mq_url
        self.msg_target: callable = msg_target

        self.state: int = MqState.INITIALIZING
        self.state_msg: str = "Attempting to initialize the subscriber"

        self.conn: SelectConnection | None = None
        self.channel: Channel | None = None
        self.queue_name: str = queue_name

        # parameter for channel QOS - for higher yield in production, try higher values
        self.prefetch_count: int = 1

        if self.logger:
            self.logger.debug(msg=(f"Instantiated, with exchange '{exchange_name}', "
                                   f"type '{exchange_type}', and queue '{queue_name}'"))

    def run(self) -> None:
        """
        Run the consumer, connecting it to *RabbitMQ*.

        The *IOLoop* is started, to block and allow the *SelectConnection* to operate.
        """
        self.conn = self.connect()
        self.conn.ioloop.start()

    def connect(self) -> SelectConnection:
        """
        Connect with *RabbitMQ*, returning the connection identifier.

        When connection is established, *on_connection_open* will be invoke by *pika*.

        :return: the connection obtained
        """
        if self.logger:
            # supress logging user and password in the URL
            #   url: <protocol>//<user>:<password>@<ip-address>
            first: int = self.mq_url.find("//")
            last = self.mq_url.find("@")
            if self.logger:
                self.logger.debug(msg=f"Connecting with '{self.mq_url[0:first]}{self.mq_url[last:]}'")

        # obtain and return the connection
        return SelectConnection(
            parameters=URLParameters(self.mq_url),
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self,
                           _unused_connection: Connection) -> None:
        """
        Account for *pika*'s *callback* invocation, when the connection with *RabbitMQ* is established.

        The identifier for the connection object is passed as parameter, in case it is needed.
        At the moment it is marked as not used.

        :param _unused_connection: the connection with RabbitMQ
        """
        self.state = MqState.CONNECTION_OPEN
        msg: str = f"Connection established: queue '{self.queue_name}'"
        self.state_msg = msg
        if self.logger:
            self.logger.debug(msg=msg)
        self.open_channel()

    def on_connection_open_error(self,
                                 _unused_connection: Connection,
                                 error: str) -> None:
        """
        Account for *pika*'s *callback* invocation, if connecting with *RabbitMQ* cannot be accomplished.

        :param _unused_connection: the attempted connection with RabbitMQ
        :param error: the associated error message
        """
        self.state = MqState.CONNECTION_ERROR
        msg: str = f"Error attempting to connect: {error}"
        self.state_msg = msg
        if self.logger:
            self.logger.error(msg=msg)
        self.reconnect()

    def on_connection_closed(self,
                             _unused_connection: Connection,
                             reason: Exception) -> None:
        """
        Account for *pika*'s *callback* invocation, when a connection with *RabbitMQ* is closed unexpectedly.

        In this situation, reconnecting with *RabbitMQ* is attempted.

        :param _unused_connection: the closed connection
        :param reason: exception indicating the reason for the connection loss
        """
        self.state = MqState.CONNECTION_CLOSED
        msg: str = f"Connection was closed: {reason}"
        self.state_msg = msg
        if self.logger:
            self.logger.warning(msg=msg)
        self.channel = None

        if self.closing:
            self.conn.ioloop.stop()
        else:
            self.reconnect()

    def close_connection(self) -> None:
        """
        Close the connection with *RabbitMQ*.
        """
        self.consuming = False
        if self.conn.is_closing:
            if self.logger:
                self.logger.debug(msg="Connection closing...")
        elif self.conn.is_closed:
            if self.logger:
                self.logger.debug(msg="Connection already closed")
        else:
            if self.logger:
                self.logger.debug(msg="Closing the connection")
            self.conn.close()

    def reconnect(self) -> None:
        """
        Account for *pika*'s *callback* invocation, if the connection is closed, or unable to open.

        Indicate that reconnecting is needed, and then interrupt the *IOloop*.
        """
        self.should_reconnect = True
        self.stop()

    def open_channel(self) -> None:
        """
        Open a new channel with *RabbitMQ*, invoking the RPC *Channel.Open* command.

        When *RabbitMQ* responds that the channel is open, the *callback* indicated in *on_channel_open_callback*
        will be invoked by *pika*.
        """
        if self.logger:
            self.logger.debug(msg="Creating a new channel...")
        self.conn.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self,
                        channel: Channel) -> None:
        """
        Account for *pika*'s *callback* invocation, when the channel is open.

        A reference to the channel object is passed as parameter, in case it is needed.
        With the channel open, the exchange to be used is declared.

        :param channel: the open channel
        """
        if self.logger:
            self.logger.debug(msg="The channel is open, the callback for its closing is established")
        self.channel = channel
        self.channel.add_on_close_callback(callback=self.on_channel_closed)
        self.setup_exchange()

    def on_channel_closed(self,
                          channel: Channel,
                          reason: Exception) -> None:
        """
        Account for *pika*'s *callback* invocation, when the channel is unexpectedly closed.

        Channels are usually closed swhen a protocol violation is attempted,
        such as re-declaring a switch or queue with different parameters.
        In this case, the connection is closed to allow for the shutdown of the object.

        :param channel: the closed channel
        :param reason: exception indicating the reason for the channel loss
        """
        if self.logger:
            self.logger.warning(msg=f"The channel '{channel}' was closed: {reason}")
        self.close_connection()

    def setup_exchange(self) -> None:
        """
        Check that the exchange is configured in *RabbitMQ*.

        This is done by invoking the RPC command *Exchange.Declare* with the parameter *passive=True*.
        If this setting is confirmed, *on_exchange_declare_ok* will be invoked by *pika*.
        """
        if self.logger:
            self.logger.debug(msg=f"Declaring the exchange '{self.exchange_name}'")
        self.channel.exchange_declare(exchange=self.exchange_name,
                                      exchange_type=self.exchange_type,
                                      passive=True,
                                      durable=True,
                                      callback=self.on_exchange_declare_ok)

    def on_exchange_declare_ok(self,
                               _unused_frame: pika_frame.Method) -> None:
        """
        Account for *pika*'s *callback* invocation, when *RabbitMQ* concludes the RPC *Exchange.Declare* command.

        :param _unused_frame: Exchange.DeclareOk response frame
        """
        if self.logger:
            self.logger.debug(msg=f"Exchange '{self.exchange_name}' declared")
        self.setup_queue()

    def setup_queue(self) -> None:
        """
        Check that the queue is configured in *RabbitMQ*.

        This is done by invoking the RPC *Queue.Declare* command with the parameter "passive=True".
        If this setting is confirmed, *on_queue_declare_ok* will be invoked by *pika*.
        """
        if self.logger:
            self.logger.debug(msg=f"Declaring the queue '{self.queue_name}'")
        self.channel.queue_declare(queue=self.queue_name,
                                   passive=True,
                                   durable=True,
                                   callback=self.on_queue_declare_ok)

    def on_queue_declare_ok(self,
                            _unused_frame: pika_frame.Method) -> None:
        """
        Account for *pika*'s *callback* invocation, when the RPC *Queue.Declare* call in *setup_queue* is finished.

        Here the queue is bound to the exchange with the given routing key,
        by the invocation of the RPC *Queue.Bind* command.
        When this binding is concluded, *on_bind_ok* will be invoked by *pika*.

        :param _unused_frame: the Queue.DeclareOk frame
        """
        if self.logger:
            self.logger.debug(msg=f"Queue '{self.queue_name}' bound to exchange '{self.exchange_name}'")
        self.setup_qos()

    def setup_qos(self) -> None:
        """
        Configure the consumer prefetch.

        This is done by defining the number of messages to be delivered, with their delivery declarations pending.
        In this case, the consumer must always signal the delivery of every message to *RabbitMQ*.
        Different prefetch values may be experimented with, so that the desired performance is achieved.
        """
        self.channel.basic_qos(prefetch_count=self.prefetch_count,
                               callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self,
                        _unused_frame: pika_frame.Method) -> None:
        """
         Account for *pika*'s *callback* invocation, when the *Basic.QoS* function is concluded.

        At this point, message consumption starts with a call to *start_consuming*,
        which will invoke the necessary RPC commands to start the process.

        :param _unused_frame: the response frame Basic.QosOk
        """
        if self.logger:
            self.logger.debug(msg=f"QOS configured to {self.prefetch_count}")
        self.start_consuming()

    def start_consuming(self) -> None:
        """
        Configure the consumer, initially by invoking *add_on_cancel_callback* in the channel.

        This is done so that the consumer be notified if *RabbitMQ* for whatever reason cancels him.
        Then the RPC *Basic.Consume* command is issued, which returns the tag used to
        uniquely identify the consumer to *RabbitMQ*. The value of this tag is retained,
        so that it can be used when canceling the consumer. The *on_message* method is passed as *callback*,
        to be invoked when new messages arrive.
        """
        if self.logger:
            self.logger.debug(msg="Adding callback for consumer cancellation")
        self.channel.add_on_cancel_callback(self.on_consumer_cancelled)

        self.consumer_tag = self.channel.basic_consume(queue=self.queue_name,
                                                       on_message_callback=self.on_message)
        self.started_consumption = True
        self.consuming = True

    def on_consumer_cancelled(self, method_frame: pika_frame.Method) -> None:
        """
        Account for *pika*'s *callback* invocation, when *RabbitMQ* sends *Basic.Cancel* to the consumer.

        :param method_frame: The Basic.Cancel frame
        """
        self.logger.debug(msg=f"The consumer has been remotely cancelled, finishing '{method_frame}'")
        if self.channel:
            self.channel.close()

    def on_message(self,
                   _unused_channel: Channel,
                   basic_deliver: pika_spec.Basic.Deliver,
                   properties: pika_spec.BasicProperties,
                   msg_body: bytes) -> None:
        """
        Account for *pika*'s *callback* invocation, when a message from *RabbitMQ* is delivered.

        The *basic_deliver* object that is passed contains the exchange, the routing key,
        the delivery label and a message resend flag. The *properties* parameter contains the message properties.
        The *msg_body* parameter contains the body of the sent message. Receipt of the message is then declared
        to *RabbitMQ* by sending the RPC *Basic.Ack* command with the corresponding delivery tag.

        :param _unused_channel: the Channel object
        :param basic_deliver: the Basic.Deliver object
        :param properties: the Spec.BasicProperties object
        :param msg_body: the body of the message
        """
        if self.logger:
            self.logger.debug(msg=(f"Msg '{basic_deliver.delivery_tag}' "
                                   f"received from '{properties.app_id}': {msg_body.decode()}"))
        self.channel.basic_ack(delivery_tag=basic_deliver.delivery_tag)

        # send message to recipient
        self.msg_target(msg_body)

    def stop_consuming(self) -> None:
        """
        Send the RPC *Basic.Cancel* command to inform *RabbitMQ* of the decision to stop consuming messages.
        """
        if self.channel:
            if self.logger:
                self.logger.debug(msg="Sending the 'RPC Basic.Cancel' command to RabbitMQ")
            self.channel.basic_cancel(consumer_tag=self.consumer_tag,
                                      callback=self.on_cancel_ok)

    def on_cancel_ok(self, _unused_frame: pika_frame.Method) -> None:
        """
        Account for *pika*'s *callback* invocation, when *RabbitMQ* acknowledges a consumer cancellation.

        At this point the channel is closed, which will cause *on_channel_closed* to be invoked,
        which in turn will close the connection.

        :param _unused_frame: The Basic.CancelOk frame
        """
        self.consuming = False
        if self.logger:
            self.logger.debug(msg=f"RabbitMQ acknowledged the consumer cancellation: '{self.consumer_tag}'")
        self.close_channel()

    def close_channel(self) -> None:
        """
        Close the channel with *RabbitMQ* cleanly, by issuing the RPC *Channel.Close* command.
        """
        if self.logger:
            self.logger.debug(msg="Closing the channel...")
        self.channel.close()

    def get_state(self) -> int:
        """
        Return the current state of the consumer.

        The state is one of:
            - MQS_CONNECTION_OPEN
            - MQS_CONNECTION_CLOSED
            - MQS_CONNECTION_ERROR
            - MQS_INITIALIZING

        :return: the current state of the consumer.
        """
        return self.state

    def get_state_msg(self) -> str:
        """
        Return the message associated with the current state of the consumer.

        :return: the state message.
        """
        return self.state_msg

    def stop(self) -> None:
        """
        Close the connection cleanly with *RabbitMQ*, stopping the consumer.

        When *RabbitMQ* confirms the cancellation, *on_cancel_ok* will be invoked by *pika*,
        which will close the channel and terminate the connection. The *IOLoop* is started again,
        because it needs to be running for *pika* to be able to communicate with *RabbitMQ*.
        All commands issued before starting *IOLoop* are stored in a buffer, but not reprocessed.
        """
        if not self.closing:
            self.closing = True
            if self.logger:
                self.logger.debug(msg="Stopping...")
            if self.consuming:
                self.stop_consuming()
                self.conn.ioloop.start()
            else:
                self.conn.ioloop.stop()
            if self.logger:
                self.logger.debug(msg="Stopped")


class _MqSubscriberMaster(threading.Thread):
    """
    Object in charge of reconnecting the consumer with *RabbitMQ*.

    This reconnection is carried out if the consumer indicates that reconnecting is necessary.
    """

    def __init__(self, mq_url: str,
                 exchange_name: str,
                 exchange_type: str,
                 queue_name: str,
                 msg_target: Callable,
                 max_reconnect_delay: int,
                 logger: Logger = None) -> None:

        threading.Thread.__init__(self)

        # initialize instance attributes
        self.mq_url: str = mq_url
        self.msg_target: Callable = msg_target
        self.queue_name: str = queue_name
        self.exchange_name = exchange_name
        self.exchange_type: str = exchange_type
        self.logger: Logger = logger
        self.reconnect_delay = 0
        self.max_reconnect_delay: int = max_reconnect_delay
        self.stopped: bool = False

        # instantiate the consumer
        self.consumer: _MqSubscriber = _MqSubscriber(mq_url=self.mq_url,
                                                     exchange_name=self.exchange_name,
                                                     exchange_type=self.exchange_type,
                                                     queue_name=self.queue_name,
                                                     msg_target=self.msg_target,
                                                     logger=self.logger)

    def run(self) -> None:
        """
        Entry point for the thread.
        """
        while True:
            # run the consumer, blocking it until it is interrupted
            self.consumer.run()

            # stop the consumer
            self.consumer.stop()

            if self.stopped or not self.__maybe_reconnect():
                break

    def stop(self) -> None:

        self.stopped = True
        if self.consumer:
            self.consumer.stop()
            self.consumer = None

    def __maybe_reconnect(self) -> bool:
        """
        Decide whether the consumer must be recreated, so that the connection can be re-established.

        :return: the decision to re-establish the connection
        """
        result: bool = self.consumer.should_reconnect

        if result:
            reconnect_delay = self.__get_reconnect_delay()
            if self.logger:
                self.logger.debug(msg=f"Reconnecting in {reconnect_delay} seconds")
            time.sleep(reconnect_delay)

            # create a new consumer instance
            self.consumer = _MqSubscriber(mq_url=self.mq_url,
                                          exchange_name=self.exchange_name,
                                          exchange_type=self.exchange_type,
                                          queue_name=self.queue_name,
                                          msg_target=self.msg_target,
                                          logger=self.logger)
        return result

    def __get_reconnect_delay(self) -> int:
        """
        Update and return the value of the reconnection delay.

        This value is incremented by 1 every time it is retrieved, until the maximum value is reached.

        :return: the reconnection delay, in seconds.
        """
        if self.consumer.started_consumption:
            self.reconnect_delay = 0
        else:
            self.reconnect_delay += 1

        self.reconnect_delay = max(self.reconnect_delay, self.max_reconnect_delay)

        return self.reconnect_delay
