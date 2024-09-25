"""Utility functions for working with YAML configuration files"""

from typing import Any

from ruamel.yaml import YAML, CommentedMap, CommentToken
from ruamel.yaml.compat import StringIO
from ruamel.yaml.error import CommentMark

from cellophane import data


class _BLANK:
    """Represents a blank value in YAML"""


def dump_yaml(data_: Any) -> str:
    """Dumps data to a YAML string"""
    with StringIO() as handle:
        yaml = YAML(typ="rt")
        # Representer for preserved dicts

        yaml.representer.add_representer(
            data.PreservedDict,
            lambda dumper, data: dumper.represent_dict(data),
        )
        # Representer for null as ~
        yaml.representer.add_representer(
            type(None),
            lambda dumper, *_: dumper.represent_scalar("tag:yaml.org,2002:null", "~"),
        )
        yaml.representer.add_representer(
            _BLANK,
            lambda dumper, *_: dumper.represent_scalar("tag:yaml.org,2002:null", ""),
        )
        yaml.dump(data_, handle)
        return handle.getvalue()


def comment_yaml_block(
    yaml: CommentedMap,
    key: tuple[str, ...] | list[str],
    level: int = 0,
) -> None:
    """Recursively comment a YAML node in-place.

    This will comment all sequence and mapping items in the node, as well as
    multi-line strings.

    Args:
    ----
        node (CommentedBase): The YAML node to comment.
        index (Any): The index of the node.
        level (int, optional): The level of the node. Defaults to 0.

    """
    # Get the parent node and the index of the leaf node
    node = yaml.mlget([*key[:-1]]) if len(key) > 1 else yaml
    index = key[-1]

    # Dump the current value as a YAML string
    lines = dump_yaml({"DUMMY": node[index]}).splitlines()

    # Use DUMMY as a placeholder key to extract any additional content on the first line
    # eg. "key: |" -> "DUMMY: |" -> "|"
    lines[0] = lines[0].removeprefix("DUMMY:").removeprefix(" ")

    # Comment and pad all lines to the current level
    for idx in range(1, len(lines)):
        lines[idx] = f"#{' ' * (level - 1) * 2} {lines[idx]}"
    commented_block = "\n".join(lines)

    # Replace current value with a blank placeholder
    node[index] = _BLANK()

    # Get the current comment token(s) for the node
    node_ca = node.ca.items.setdefault(index, [None, []])

    # If there are comments, pad them
    for comment in node_ca[1]:
        comment.value = f"#{' ' * comment.column} {comment.value}"
        comment.start_mark = CommentMark(0)

    # Add a comment token before the node key
    node_ca[1].append(CommentToken("# ", CommentMark(0)))

    # Add back value as a comment token
    node_ca.append(CommentToken(commented_block, CommentMark(0)))
