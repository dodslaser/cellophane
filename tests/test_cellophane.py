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
    @testing.parametrize_from_yaml(
        [
            LIB / "integration" / "good_basic.yaml",
            LIB / "integration" / "good_no_runners.yaml",
            LIB / "integration" / "good_hooks.yaml",
            LIB / "integration" / "good_merge.yaml",
            LIB / "integration" / "good_outputs.yaml",
            LIB / "integration" / "bad_missing_file.yaml",
            LIB / "integration" / "bad_hook_order.yaml",
            LIB / "integration" / "bad_unhandled_exception.yaml",
            LIB / "integration" / "bad_runner_unhandled_exception.yaml",
            LIB / "integration" / "bad_schema.yaml",
            LIB / "integration" / "bad_args.yaml",
            LIB / "integration" / "bad_module.yaml",
            LIB / "integration" / "bad_keyboard_interrupt.yaml",
        ],
    )
    def test_cellophane(
        definition: Path,
        run_definition,
    ):
        run_definition(definition)
