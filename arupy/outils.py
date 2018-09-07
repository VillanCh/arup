#!/usr/bin/env python3
# coding:utf-8
import sys
import os
import logging
from logging import StreamHandler
from datetime import datetime
import colorama

colorama.init(autoreset=True)

_LOGGER_NAME = 'cli'
_LOGGER_FMT = '[%(asctime)s] [%(name)s] %(levelname)s : %(message)s'
CLEAR_LINE = '\033[K'
CLEAR_SCREEN = '\033[J'

_loggers = {}


def get_stty_size():
    return os.popen('stty size', 'r').read().split()


def green(msg):
    return colorama.Fore.GREEN + msg


def yellow(msg):
    return colorama.Fore.YELLOW + msg


def red(msg):
    return colorama.Fore.RED + msg


def blue(msg):
    return colorama.Fore.BLUE + msg


def bright(msg):
    return colorama.Style.BRIGHT + msg


def dim(msg):
    return colorama.Style.DIM + msg


def _newline(msg):
    return "{}{}".format(CLEAR_LINE, msg)


def clear_line():
    print("\r" + colorama.ansi.clear_line(), end="")


def clear_screen():
    print("\r" + colorama.ansi.clear_screen(), end="\r")


class _FilterByLevel(logging.Filter):
    """"""

    def __init__(self, name="", needlevel=""):
        logging.Filter.__init__(self, name)
        self._needlevel = needlevel

    def filter(self, record):
        return record.levelno == self._needlevel


def _set_logger(logger: logging.Logger):
    level2color = {
        logging.DEBUG: blue,
        logging.INFO: green,
        logging.WARN: yellow,
        logging.ERROR: red,
        logging.CRITICAL: [bright, red],
        logging.FATAL: [bright, red],
    }

    def _set(level, handler):
        msg = _LOGGER_FMT
        if callable(handler):
            msg = handler(msg)
        elif isinstance(handler, (tuple, list)):
            for _subhandler in handler:
                msg = _subhandler(msg)
        fmt = logging.Formatter(msg)
        stdouthandler = logging.StreamHandler(sys.stdout)
        stdouthandler.setLevel(level)
        stdouthandler.setFormatter(fmt)
        stdouthandler.addFilter(
            _FilterByLevel(needlevel=level)
        )
        return stdouthandler

    handlers = [_set(level, handler) for (level, handler) in level2color.items()]
    [logger.addHandler(hdlr) for hdlr in handlers]
    return logger


def println(msg="", *vargs, **kwargs):
    print(_newline(msg))


def lastline(msg):
    print(_newline(msg), end='\r')


def get_logger(logger_name=None):
    logger_name = logger_name or _LOGGER_NAME
    if logger_name not in _loggers:
        logger = logging.getLogger(logger_name)
        _set_logger(logger)
        _loggers[logger_name] = logger
        return logger
    else:
        return _loggers.get(logger_name)


def __test():
    println("test")
    println(green("GREEN TEST"))
    print("normal")
    println(yellow("YELLOW TEST"))
    println(red("RED TEST"))
    println(blue("RED TEST"))
    println(red(bright("RED TEST")))
    println(red(dim("RED TEST")))
    println(red(dim("RED TEST")))
    print('TEST', end="")
    clear_line()
    print("NEWLONE")
    clear_screen()

    logger = get_logger()
    logger.setLevel(logging.DEBUG)
    logger.debug("test")
    logger.info("test")
    logger.warn("test")
    logger.error("test")
    logger.fatal("test")
    logger.critical("test")


if __name__ == "__main__":
    __test()
