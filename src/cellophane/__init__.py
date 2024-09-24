"""Cellophane: A library for writing modular wrappers"""

from .cellophane import CELLOPHANE_ROOT, CELLOPHANE_VERSION, cellophane
from . import cfg, data, executors, logs, modules, util
from .cfg import Config, Schema
from .cleanup import Cleaner
from .data import Output, OutputGlob, Sample, Samples
from .executors import Executor
from .modules import Checkpoint, Checkpoints, output, post_hook, pre_hook, runner

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
    # checkpoints
    "Checkpoint",
    "Checkpoints",
    # cleanup
    "Cleaner",
]
