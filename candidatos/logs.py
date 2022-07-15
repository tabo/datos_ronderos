import logging
from typing import Any, List

import structlog
from structlog.stdlib import ProcessorFormatter
from structlog.types import Processor


def _get_logger() -> Any:
    shared_processors: List[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.CallsiteParameterAdder({structlog.processors.CallsiteParameter.THREAD_NAME})
    ]
    structlog_processors: List[Processor] = shared_processors + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ]

    structlog.configure(
        processors=structlog_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = ProcessorFormatter(
        # These run ONLY on `logging` entries that do NOT originate within
        # structlog.
        foreign_pre_chain=shared_processors,
        # These run on ALL entries after the pre_chain is done.
        processors=[
            # Remove _record & _from_structlog.
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(),
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.DEBUG)

    return structlog.get_logger()


log: Any = _get_logger()
