import sys
import time
from logging import Logger
from pypomes_core import exc_format
from typing import Final

from .mq_config import MqConfig, MqState
from .mq_subscriber import _MqSubscriberMaster

__DEFAULT_BADGE: Final[str] = "__default__"

# dict holding the subscribers created:
#   <{ <badge-1>: <subscriber-master-instance-1>,
#     ...
#     <badge-n>: <subscriber-master-instance-n>
#   }>
__subscribers: dict = {}


def subscriber_create(queue_name: str,
                      msg_target: callable,
                      badge: str = None,
                      is_daemon: bool = True,
                      max_reconnect_delay: int = int(MqConfig.MAX_RECONNECT_DELAY),
                      errors: list[str] = None,
                      logger: Logger = None) -> None:
    """
    Create the asynchronous subscriber.

    This is a wrapper around the package *Pika*, an implementation for a *RabbitMQ* client.

    :param queue_name: queue to use
    :param msg_target: the callback to reach the messager cosumer
    :param badge: optional badge identifying the publisher
    :param is_daemon: whether the subscriber thread is a daemon thread
    :param max_reconnect_delay: maximum delay for re-establishing lost connections, in seconds
    :param errors: incidental errors
    :param logger: optional logger
    :return: True if the subscriber was created, or False otherwise
    """
    # define the badge
    curr_badge: str = badge or __DEFAULT_BADGE

    # has the subscriber been instantiated ?
    if __get_subscriber(badge=curr_badge,
                        must_exist=False,
                        errors=errors) is None:
        # no, instantiate it
        try:
            __subscribers[curr_badge] = _MqSubscriberMaster(mq_url=MqConfig.CONNECTION_URL,
                                                            exchange_name=MqConfig.EXCHANGE_NAME,
                                                            exchange_type=MqConfig.EXCHANGE_TYPE,
                                                            queue_name=f"{MqConfig.ROUTING_BASE}.{queue_name}",
                                                            msg_target=msg_target,
                                                            max_reconnect_delay=max_reconnect_delay,
                                                            logger=logger)
            if is_daemon:
                __subscribers[curr_badge].daemon = True
        except Exception as e:
            msg: str = (f"Error creating the subscriber '{badge or __DEFAULT_BADGE}': "
                        f"{exc_format(e, sys.exc_info())}")
            if isinstance(errors, list):
                errors.append(msg)
            if logger:
                logger.error(msg=msg)


def subscriber_destroy(badge: str = None) -> None:
    """
    Destroy the subscriber identified by *badge*. *Noop* if the subscriber does not exist.

    :param badge: optional badge identifying the scheduler
    """
    # define the badge and retrieve the corresponding subscriber
    curr_badge: str = badge or __DEFAULT_BADGE
    subscriber: _MqSubscriberMaster = __subscribers.get(curr_badge)

    # does it exist ?
    if subscriber:
        # yes, stop and discard it
        subscriber.stop()
        __subscribers.pop(curr_badge)


def subscriber_start(badge: str = None,
                     errors: list[str] = None) -> bool:
    """
    Start the subscriber identified by *badge*.

    :param badge: optional badge identifying the publisher
    :param errors: incidental errors
    :return: True if the publisher has been started, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(badge=badge,
                                                       errors=errors)
    if subscriber:
        started: bool = False
        try:
            subscriber.start()
            started = True
        except Exception as e:
            msg: str = (f"Error starting the subscriber '{badge or __DEFAULT_BADGE}': "
                        f"{exc_format(e, sys.exc_info())}")
            if isinstance(errors, list):
                errors.append(msg)
            if subscriber.logger:
                subscriber.logger.error(msg=msg)

        # was it started ?
        if not started:
            # no, wait for the conclusion
            while subscriber.consumer.get_state() == MqState.INITIALIZING:
                time.sleep(0.001)

            # did connecting with the subscriber fail ?
            if subscriber.consumer.get_state() == MqState.CONNECTION_ERROR:
                # yes, report the error
                msg: str = (f"Error starting the subscriber '{badge or __DEFAULT_BADGE}': "
                            f"{subscriber.consumer.get_state_msg()}")
                if isinstance(errors, list):
                    errors.append(msg)
                if subscriber.logger:
                    subscriber.logger.error(msg=msg)
            else:
                # no, report success
                result = True

    return result


def subscriber_stop(badge: str = None,
                    errors: list[str] = None) -> bool:
    """
    Stop the subscriber identified by *badge*.

    :param badge: optional badge identifying the subscriber
    :param errors: incidental errors
    :return: True if the subscriber has been stopped, False otherwise
    """
    # initialize the return variable
    result: bool = False

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(badge=badge,
                                                       errors=errors)
    if subscriber:
        subscriber.stop()
        result = True

    return result


def subscriber_get_state(badge: str = None,
                         errors: list[str] = None) -> int:
    """
    Retrieve the current state of the subscriber identified by *badge*.

    :param badge: optional badge identifying the subscriber
    :param errors: incidental errors
    :return: the current state of the subscriber
    """
    # initialize the return variable
    result: int | None = None

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(badge=badge,
                                                       errors=errors)
    if subscriber:
        result = subscriber.consumer.get_state()

    return result


def subscriber_get_state_msg(errors: list[str],
                             badge: str = None) -> str:
    """
    Retrieve the message associated with the current state of the subscriber identified by *badge*.

    :param badge: optional badge identifying the subscriber
    :param errors: incidental errors
    :return: the message associated with the current state of the subscriber
    """
    # initialize the return variable
    result: str | None = None

    # retrieve the subscriber
    subscriber: _MqSubscriberMaster = __get_subscriber(badge=badge,
                                                       errors=errors)
    if subscriber:
        result = subscriber.consumer.get_state_msg()

    return result


def __get_subscriber(badge: str,
                     must_exist: bool = True,
                     errors: list[str] = None) -> _MqSubscriberMaster:
    """
    Retrieve the subscriber identified by *badge*.

    :param badge: optional badge identifying the publisher
    :param must_exist: True if publisher must exist
    :param errors: incidental errors
    :return: the publisher retrieved, or None otherwise
    """
    curr_badge = badge or __DEFAULT_BADGE
    result: _MqSubscriberMaster = __subscribers.get(curr_badge)
    if must_exist and not result and isinstance(errors, list):
        errors.append(f"Subscriber '{curr_badge}' has not been created")

    return result
