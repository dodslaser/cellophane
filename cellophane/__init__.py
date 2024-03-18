"""Cellophane: A library for writing modular wrappers"""

from .cellophane import CELLOPHANE_ROOT, CELLOPHANE_VERSION, cellophane
from .src import cfg, data, executors, logs, modules, util
from .src.cfg import Config, Schema
from .src.data import Output, OutputGlob, Sample, Samples
from .src.executors import Executor
from .src.modules import output, post_hook, pre_hook, runner

__all__ = [
    "CELLOPHANE_ROOT",
    "CELLOPHANE_VERSION",
    "cellophane",
    "cfg",
    "data",
    "logs",
    "modules",
    "util",
    "executors",
    # modules
    "output",
    "post_hook",
    "pre_hook",
    "runner",
    # data
    "Output",
    "OutputGlob",
    "Sample",
    "Samples",
    # executors
    "Executor",
    # cfg
    "Config",
    "Schema",
]
