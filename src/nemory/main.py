import logging

from nemory.cli.commands import nemory

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        nemory(obj={})
    except Exception as e:
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception(e)
        else:
            logger.error(str(e))


if __name__ == "__main__":
    main()
