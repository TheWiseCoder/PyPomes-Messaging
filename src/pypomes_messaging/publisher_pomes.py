import logging
import time
import sys
from typing import Final
from pypomes_core import APP_PREFIX, env_get_str, env_get_int, exc_format
from .mq_publisher import (
    MQP_CONNECTION_ERROR, MQP_INITIALIZING,
    _MqPublisher
)

__DEFAULT_BADGE: Final[str] = "__default__"

# environment variables
MQ_CONNECTION_URL: Final[str] = env_get_str(f"{APP_PREFIX}_MQ_CONNECTION_URL")
MQ_EXCHANGE_NAME: Final[str] = env_get_str(f"{APP_PREFIX}_MQ_EXCHANGE_NAME")
MQ_EXCHANGE_TYPE: Final[str] = env_get_str(f"{APP_PREFIX}_MQ_EXCHANGE_TYPE")
MQ_ROUTING_BASE: Final[str] = env_get_str(f"{APP_PREFIX}_MQ_ROUTING_BASE")
MQ_MAX_RECONNECT_DELAY: Final[int] = env_get_int(f"{APP_PREFIX}_MQ_MAX_RECONNECT_DELAY", 30)

# dict holding the publishers created:
#   <{ <badge-1>: <publisher-instance-1>,
#     ...
#     <badge-n>: <publisher-instance-n>
#   }>
__publishers: dict = {}


def publisher_create(errors: list[str], max_reconnect_delay: int = MQ_MAX_RECONNECT_DELAY,
                     logger: logging.Logger = None, badge: str = None) -> bool:
    """
    Create the threaded events publisher.

    This is a wrapper around the package *Pika*, an implementation for a *RabbitMQ* client.

    :param errors: incidental errors
    :param logger: optional logger object
    :param max_reconnect_delay: maximum delay for re-establishing lost connections, in seconds
    :param badge: optional badge identifying the publisher
    :return: True if the publisher was created, or False otherwise
    """
    # initialize the return variable
    result: bool = False

    # define the badge
    curr_badge: str = badge or __DEFAULT_BADGE

    # has the scheduler been instantiated ?
    if __get_publisher(errors, curr_badge, False) is None:
        # no, instantiate it
        __publishers[curr_badge] = _MqPublisher(MQ_CONNECTION_URL, MQ_EXCHANGE_NAME,
                                                MQ_EXCHANGE_TYPE, max_reconnect_delay, logger)
        __publishers[curr_badge].daemon = True

    return result


def publisher_destroy(badge: str = None) -> None:
    """
    Destroy the publisher identified by *badge*. *Noop* if the publisher does not exist.

    :param badge: optional badge identifying the scheduler
    """
    # define the badge and retrieve the corresponding publisher
    curr_badge: str = badge or __DEFAULT_BADGE
    publisher: _MqPublisher = __publishers.get(curr_badge)

    # does the scheduler exist ?
    if publisher is not None:
        # yes, stop and discard it
        publisher.stop()
        __publishers.pop(curr_badge)


def publisher_start(errors: list[str], badge: str = None) -> bool:
    """
    Start the publisher identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :return: True if the publisher has been started, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the publisher
    publisher: _MqPublisher = __get_publisher(errors, badge)

    # proceed, if the publisher was retrieved
    if publisher is not None:
        try:
            publisher.start()
        except Exception as e:
            errors.append(f"Error starting the events publisher '{badge or __DEFAULT_BADGE}': "
                          f"{exc_format(e, sys.exc_info())}")

        # were there errors ?
        if len(errors) == 0:
            # no, wait for the conclusion
            while publisher.get_state() == MQP_INITIALIZING:
                time.sleep(0.001)

            # did connecting with the publisher fail ?
            if publisher.get_state() == MQP_CONNECTION_ERROR:
                # yes, report the error
                errors.append(f"Error starting the events publisher '{badge or __DEFAULT_BADGE}': "
                              f"{publisher.get_state_msg()}")

    return result


def publisher_stop(errors: list[str], badge: str = None) -> bool:
    """
    Stop the publisher identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :return: True if the publisher has been stopped, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the publisher
    publisher: _MqPublisher = __get_publisher(errors, badge)

    # proceed, if the publisher was retrieved
    if publisher is not None:
        publisher.stop()
        result = True

    return result


def publisher_get_state(errors: list[str], badge: str = None) -> int:
    """
    Retrieve and return the current state of the publisher identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :return: the current state of the publisher
    """
    # initialize the return variable
    result: int | None = None

    # retrieve the publisher
    publisher: _MqPublisher = __get_publisher(errors, badge)

    # proceed, if the publisher was retrieved
    if publisher is not None:
        result = publisher.get_state()

    return result


def publisher_get_state_msg(errors: list[str], badge: str = None) -> str:
    """
    Retrieve and return the message associated with the current state of the publisher identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :return: the message associated with the current state of the publisher
    """
    # initialize the return variable
    result: str | None = None

    # retrieve the publisher
    publisher: _MqPublisher = __get_publisher(errors, badge)

    # proceed, if the publisher was retrieved
    if publisher is not None:
        result = publisher.get_state_msg()

    return result


def publisher_publish(errors: list[str], msg_body: str | bytes, routing_key: str,
                      msg_mimetype: str = "application/text", msg_headers: str = None, badge: str = None) -> bool:
    """
    Send a message to the publisher identified by *badge*, for publishing.

    :param errors: incidental errors
    :param msg_body: body of the message
    :param routing_key: key for message routing
    :param msg_mimetype: message mimetype (defaults to type text)
    :param msg_headers: optional message headers
    :param badge:  optional badge identifying the publisher
    :return: True if the message was published, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the publisher
    publisher: _MqPublisher = __get_publisher(errors, badge)
    
    # proceed, if the publisher was retrieved
    if publisher is not None:
        try:
            publisher.publish_message(errors, msg_body,
                                      f"{MQ_ROUTING_BASE}.{routing_key}", msg_mimetype, msg_headers)
            result = True
        except Exception as e:
            errors.append(f"Error publishing message: {exc_format(e, sys.exc_info())}")

    return result


def __get_publisher(errors: list[str], badge: str, must_exist: bool = True) -> _MqPublisher:
    """
    Retrieve the publisher identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :param must_exist: True if publisher must exist
    :return: the publisher retrieved, or None otherwise
    """
    curr_badge = badge or __DEFAULT_BADGE
    result: _MqPublisher = __publishers.get(curr_badge)
    if must_exist and result is None:
        errors.append(f"Publisher '{curr_badge}' has not been created")

    return result
