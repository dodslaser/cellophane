import sys
from pathlib import Path

from cellophane import cellophane

if __name__ == "__main__":
    _root = Path(__file__).parent
    sys.path.append(str(_root))
    _main = cellophane("{label}", root=_root)
    _main(prog_name="{prog_name}")  # pylint: disable=no-value-for-parameter
