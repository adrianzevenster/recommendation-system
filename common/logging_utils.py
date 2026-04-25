import logging
import sys
from pythonjsonlogger import jsonlogger


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        log_record.setdefault("level", record.levelname)
        log_record.setdefault("logger", record.name)
        log_record.setdefault("service", getattr(record, "service", None))


def configure_logging(service_name: str, level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stdout)
    formatter = CustomJsonFormatter('%(asctime)s %(level)s %(name)s %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    logging.getLogger("confluent_kafka").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    class ServiceFilter(logging.Filter):
        def filter(self, record):
            record.service = service_name
            return True

    handler.addFilter(ServiceFilter())
