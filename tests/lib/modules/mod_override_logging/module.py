from logging import root, Handler

root.handlers = [*root.handlers, Handler()]
