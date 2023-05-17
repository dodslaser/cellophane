import sys
from cellophane import cellophane
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).parent
    sys.path.append(str(_root))
    _main = cellophane("{label}", root=_root)
    _main(prog_name="{prog_name}")
