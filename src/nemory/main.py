import logging

from nemory.cli.commands import nemory

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        nemory(obj={})
    except Exception as e:
        # Logs the full stack trace to any logger set at debug level
        logger.debug(str(e), exc_info=True, stack_info=True)
        logger.error(str(e))


if __name__ == "__main__":
    main()
