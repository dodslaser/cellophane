"""Schema for configuration data"""

from functools import cached_property
from pathlib import Path
from typing import Any, Sequence

from ruamel.yaml import YAML, CommentedMap, CommentToken
from ruamel.yaml.error import CommentMark
from ruamel.yaml.scalarstring import DoubleQuotedScalarString, LiteralScalarString

from cellophane.src import data, util

from .jsonschema_ import get_flags
from .util import comment_yaml_block, dump_yaml


class Schema(data.Container):
    """
    Represents a schema for configuration data.

    Class Methods:
        from_file(cls, path: Path | Sequence[Path]) -> Schema:
            Loads the schema from a file or a sequence of files.

    Properties:
        add_options: Decorator that adds click options to a function.
        flags: List of flags extracted from the schema.
        example_config: Example configuration generated from the schema.

    Example:
        ```python
        schema = Schema()

        # Loading schema from a file
        path = Path("schema.yaml")
        loaded_schema = Schema.from_file(path)

        # Adding click options to a function
        @schema.add_options
        @click.command()
        def cli(**kwargs):
            ...

        # Getting the list of flags
        flags = schema.flags

        # Generating an example configuration
        example_config = schema.example_config
        ```
    """

    @classmethod
    def from_file(cls, path: Path | Sequence[Path]) -> "Schema":
        """Loads the schema from a file or a sequence of files"""
        if isinstance(path, Path):
            with open(path, "r", encoding="utf-8") as handle:
                return cls(YAML(typ="safe").load(handle) or {})
        elif isinstance(path, Sequence):
            schema: dict = {}
            for p in path:
                schema = util.merge_mappings(schema, data.as_dict(cls.from_file(p)))
            return cls(schema)

    @cached_property
    def example_config(self) -> str:
        """Generate an example configuration from the schema"""
        _map = CommentedMap()

        # Generate a list of flags and all of their parent nodes
        _nodes_flags = [
            ({(*flag.key[: i + 1],) for i in range(len(flag.key))}, flag)
            for flag in get_flags(self, {})
        ]

        # Keep track of nodes to be commented
        to_be_commented = {i for n, _ in _nodes_flags for i in n}

        for node_keys, flag in _nodes_flags:
            current_node = _map
            for k in flag.key[:-1]:
                current_node.setdefault(k, CommentedMap())
                current_node = current_node[k]

            # FIXME: Can this be moved somewhere else?
            _default: Any
            if isinstance(flag.default, str):
                if "\n" in flag.default:
                    # If the string is multi-line, use | to preserve newlines
                    _default = LiteralScalarString(flag.default)
                else:
                    # Otherwise, use " to preserve whitespace
                    _default = DoubleQuotedScalarString(flag.default)
            else:
                # For all other types, use the default string representation
                _default = flag.default

            # Add the flag to the current node
            current_node.insert(1, flag.key[-1], _default)

            # Construct item description as a comment token
            if flag.description:
                _comment = f"{flag.description} [{flag.type}]"
            else:
                _comment = f"[{flag.type}]"

            # Add a REQUIRED tag to the comment if the flag is required
            # and remove the node from the list of nodes to be commented
            if flag.required:
                _comment = f"{_comment} (REQUIRED)"
                to_be_commented -= node_keys

            # Add a comment token before the flag
            current_node.ca.items.setdefault(flag.key[-1], [None, []])
            current_node.ca.items[flag.key[-1]][1] = [
                CommentToken(f"# {_comment}\n", CommentMark((len(flag.key) - 1) * 2))
            ]

        # raise Exception(to_be_commented)
        for node_key in to_be_commented:
            # Only comment the top-most node
            if node_key[:-1] not in to_be_commented:
                comment_yaml_block(_map, node_key, level=len(node_key))

        # Dump the map to a string
        return dump_yaml(_map)
