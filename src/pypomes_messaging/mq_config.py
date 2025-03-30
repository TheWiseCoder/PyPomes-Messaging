from enum import IntEnum, StrEnum
from pypomes_core import APP_PREFIX, env_get_str


class MqConfig(StrEnum):
    """
    MQ configuration values.
    """
    CONNECTION_URL = env_get_str(key=f"{APP_PREFIX}_MQ_CONNECTION_URL",
                                 def_value="")
    EXCHANGE_NAME = env_get_str(key=f"{APP_PREFIX}_MQ_EXCHANGE_NAME",
                                def_value="")
    EXCHANGE_TYPE = env_get_str(key=f"{APP_PREFIX}_MQ_EXCHANGE_TYPE",
                                def_value="")
    ROUTING_BASE = env_get_str(key=f"{APP_PREFIX}_MQ_ROUTING_BASE",
                               def_value="")
    MAX_RECONNECT_DELAY = env_get_str(key=f"{APP_PREFIX}_MQ_MAX_RECONNECT_DELAY",
                                      def_value="30")


class MqState(IntEnum):
    """
    MQ Publisher's runtime state values.
    """
    CONNECTION_OPEN = 1
    CONNECTION_CLOSED = 2
    CONNECTION_ERROR = -1
    INITIALIZING = 0

