from pathlib import Path

from cellophane.src import testing

LIB = Path(__file__).parent / "lib"

class Test_cellophane:
    """
    This test suite serves as a small integration test for cellophane.

    It is not meant to be exhaustive, but rather to ensure that the
    basic functionality works as expected from the command line interface.
    """

    @staticmethod
    @testing.parametrize_from_yaml([*LIB.glob("integration/*.yaml")])
    def test_cellophane(
        definition: Path,
        run_definition,
    ):
        run_definition(definition)
