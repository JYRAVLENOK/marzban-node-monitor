from monitor.monitor import NodeMonitor
from monitor.config import Config
import logging

# Преобразуем строковый уровень в константу logging
LOG_LEVEL_MAPPING = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

log_level = LOG_LEVEL_MAPPING.get(Config.LOG_LEVEL, logging.INFO)

logging.basicConfig(
    level=log_level,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

if __name__ == "__main__":
    monitor = NodeMonitor()
    monitor.monitor()
