import logging
import sys
import time
from pypomes_core import exc_format
from typing import Final
from .publisher_pomes import (
    MQ_CONNECTION_URL, MQ_EXCHANGE_NAME,
    MQ_EXCHANGE_TYPE, MQ_ROUTING_BASE, MQ_MAX_RECONNECT_DELAY
)
from .mq_subscriber import (
    MQS_CONNECTION_ERROR, MQS_INITIALIZING,
    _MqSubscriberMaster
)

__DEFAULT_BADGE: Final[str] = "__default__"

# dict holding the subscribers created:
#   <{ <badge-1>: <subscriber-master-instance-1>,
#     ...
#     <badge-n>: <subscriber-master-instance-n>
#   }>
__subscribers: dict = {}


def subscriber_create(errors: list[str], queue_name: str, msg_target: callable,
                      max_reconnect_delay: int = MQ_MAX_RECONNECT_DELAY,
                      logger: logging.Logger = None, badge: str = None) -> bool:
    """
    Create the threaded events subscriber.

    This is a wrapper around the package *Pika*, an implementation for a *RabbitMQ* client.

    :param errors: incidental errors
    :param queue_name: queue to use
    :param msg_target: the callback to reach the messager cosumer
    :param max_reconnect_delay: maximum delay for re-establishing lost connections, in seconds
    :param logger: optional logger object
    :param badge: optional badge identifying the publisher
    :return: True if the subscriber was created, or False otherwise
    """
    # initialize the return variable
    result: bool = False

    # define the badge
    curr_badge: str = badge or __DEFAULT_BADGE

    # has the scheduler been instantiated ?
    if __get_subscriber(errors, curr_badge, False) is None:
        # no, instantiate it
        __subscribers[curr_badge] = _MqSubscriberMaster(MQ_CONNECTION_URL, MQ_EXCHANGE_NAME,
                                                        MQ_EXCHANGE_TYPE, f"{MQ_ROUTING_BASE}.{queue_name}",
                                                        msg_target, max_reconnect_delay, logger)
        __subscribers[curr_badge].daemon = True

    return result


def subscriber_destroy(badge: str = None) -> None:
    """
    Destroy the subscriber identified by *badge*. *Noop* if the subscriber does not exist.

    :param badge: optional badge identifying the scheduler
    """
    # define the badge and retrieve the corresponding subscriber
    curr_badge: str = badge or __DEFAULT_BADGE
    subscriber: _MqSubscriberMaster = __subscribers.get(curr_badge)

    # does the subscriber exist ?
    if subscriber is not None:
        # yes, stop and discard it
        subscriber.stop()
        __subscribers.pop(curr_badge)


def subscriber_start(errors: list[str], badge: str = None) -> bool:
    """
    Start the subscriber identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :return: True if the publisher has been started, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the publisher
    subscriber: _MqSubscriberMaster = __get_subscriber(errors, badge)

    # proceed, if the subscriber was retrieved
    if subscriber is not None:
        try:
            subscriber.start()
        except Exception as e:
            errors.append(f"Error starting the subscriber '{badge or __DEFAULT_BADGE}': "
                          f"{exc_format(e, sys.exc_info())}")

        # were there errors ?
        if len(errors) == 0:
            # no, wait for the conclusion
            while subscriber.consumer.get_state() == MQS_INITIALIZING:
                time.sleep(0.001)

            # did connecting with the subscriber fail ?
            if subscriber.consumer.get_state() == MQS_CONNECTION_ERROR:
                # yes, report the error
                errors.append(f"Error starting the subscriber '{badge or __DEFAULT_BADGE}': "
                              f"{subscriber.consumer.get_state_msg()}")

    return result


def subscriber_stop(errors: list[str], badge: str = None) -> bool:
    """
    Stop the subscriber identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the subscriber
    :return: True if the subscriber has been stopped, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the publisher
    subscriber: _MqSubscriberMaster = __get_subscriber(errors, badge)

    # proceed, if the publisher was retrieved
    if subscriber is not None:
        subscriber.stop()
        result = True

    return result


def __get_subscriber(errors: list[str], badge: str, must_exist: bool = True) -> _MqSubscriberMaster:
    """
    Retrieve the subscriber identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the publisher
    :param must_exist: True if publisher must exist
    :return: the publisher retrieved, or None otherwise
    """
    curr_badge = badge or __DEFAULT_BADGE
    result: _MqSubscriberMaster = __subscribers.get(curr_badge)
    if must_exist and result is None:
        errors.append(f"Subscriber '{curr_badge}' has not been created")

    return result


def subscriber_get_state(errors: list[str], badge: str = None) -> int:
    """
    Retrieve and return the current state of the subscriber identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the subscriber
    :return: the current state of the subscriber
    """
    # initialize the return variable
    result: int | None = None

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(errors, badge)

    # proceed, if the publisher was retrieved
    if subscriber is not None:
        result = subscriber.consumer.get_state()

    return result


def subscriber_get_state_msg(errors: list[str], badge: str = None) -> str:
    """
    Retrieve and return the message associated with the current state of the subscriber identified by *badge*.

    :param errors: incidental errors
    :param badge: optional badge identifying the subscriber
    :return: the message associated with the current state of the subscriber
    """
    # initialize the return variable
    result: str | None = None

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(errors, badge)

    # proceed, if the publisher was retrieved
    if subscriber is not None:
        result = subscriber.consumer.get_state_msg()

    return result
