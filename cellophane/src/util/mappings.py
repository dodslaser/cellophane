"""Unitility functions for working with mappings."""

from typing import Any, Hashable


def map_nested_keys(
    node: dict[str, Any] | Any,
    path: tuple[str, ...] | None = None,
) -> tuple[tuple[str, ...], ...]:
    """
    Maps the keys of a nested mapping.

    Args:
        data (Any): Mapping for which to map the nested keys.

    Returns:
        tuple[tuple[str, ...]]: A tuple of tuples of the paths to mapping keys.

    Example:
        ```python
        data = {
            "key1": {
                "key2": "value1",
                "key3": "value2"
            },
            "key4": {
                "key5": "value3"
            }
        }

        map_nested_keys(data)   # (("key1", "key2"), ("key1", "key3"), ("key4", "key5"))
        ```
    """
    if path is None:  # For the root node
        path = ()

    if not isinstance(node, dict) or len(node) == 0:
        return (path,) if path else ()

    paths: list[tuple[str, ...]] = []
    for key in node:
        # Add the current key to the path
        new_path = (*path, key)
        # Recurse on child nodes and extend paths
        paths.extend(map_nested_keys(node[key], new_path))

    return tuple(paths)


def merge_mappings(m_1: Any, m_2: Any) -> Any:
    """
    Merges two nested mappings into a single mapping.

    Args:
        m_1 (Any): The first mapping.
        m_2 (Any): The second mapping.

    Returns:
        Any: The merged mapping.

    Example:
        ```python
        m_1 = {"k1": "v1", "k2": ["v2", "v3"]}
        m_2 = {"k2": ["v4", "v5"], "k3": "v6"}
        merge_mappings(m_1, m_2)

        # {
        #     "k1": "v1",
        #     "k2": ["v2", "v3", "v4", "v5"],
        #     "k3": "v6"
        # }
        ```
    """
    match m_1, m_2:
        case {**m_1}, {**m_2} if not any(k in m_1 for k in m_2):
            return m_1 | m_2
        case {**m_1}, {**m_2}:
            return {k: merge_mappings(v, m_2.get(k, v)) for k, v in (m_2 | m_1).items()}
        case [{**m_1}], [{**m_2}]:
            return [merge_mappings(m_1, m_2)]
        case [*m_1], [*m_2] if all(isinstance(v, Hashable) for v in m_1 + m_2):
            # dict is used to preserve order while removing duplicates
            # FIXME: Is this always the desired behavior?
            return [*dict.fromkeys(m_1 + m_2)]
        case _:
            return m_2
