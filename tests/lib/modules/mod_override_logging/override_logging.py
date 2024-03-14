# type: ignore
# pylint: disable=all

from logging import Handler, root

root.handlers = [*root.handlers, Handler()]
