import logging

from databao_context_engine.cli.commands import dce

logger = logging.getLogger(__name__)


def main() -> None:
    try:
        dce(obj={})
    except Exception as e:
        if logger.isEnabledFor(logging.DEBUG):
            logger.exception(e)
        else:
            logger.error(str(e))


if __name__ == "__main__":
    main()
