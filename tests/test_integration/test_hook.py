from pytest import mark

from cellophane.testing import BaseTest, Invocation, literal


class Test_hooks(BaseTest):
    args = ["--workdir out"]

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "modules/a.py": """
                from cellophane import Sample, Samples, runner, post_hook, pre_hook
                from attrs import define, field

                @define(slots=False, init=False)
                class SampleMixin(Sample):
                    group: str | None = None
                    var: list[str] = field(factory=list)

                    @Sample.merge.register("var")
                    @staticmethod
                    def merge_var(this: list[str], that: list[str]):
                        return [*this, *that]

                @define(slots=False, init=False)
                class SamplesMixin(Samples):
                    var: list[str] = field(factory=list)

                    @Samples.merge.register("var")
                    @staticmethod
                    def merge_var(this: list[str], that: list[str]):
                        return [*this, *that]


                @runner(split_by="group")
                def runner_a(samples, logger, **_):
                    tag = "runner_a"
                    samples.var.append(tag)

                    for sample in samples:
                        sample.var.append(tag)
                        if sample.files[0].read_text() == "FAIL":
                            sample.fail("DUMMY FAIL")
                    return samples

                @runner(split_by="group")
                def runner_b(samples, logger, **_):
                    tag = "runner_b"
                    samples.var.append(tag)
                    logger.info(f"{tag}: {sorted(samples.unique_ids)}")
                    for sample in samples:
                        sample.var.append(tag)
                    return samples

                def hook_factory(tag):
                    def _hook(samples, logger, **_):
                        samples.var.append(tag)
                        logger.info(f"{tag}: {sorted(samples.unique_ids)}")
                        for sample in samples:
                            sample.var.append(tag)
                        return samples
                    _hook.__name__ = tag
                    return _hook

                @pre_hook(before="all")
                def setup_hook(samples, logger, **_):
                    for sample in samples:
                        if sample.id == "fail_early":
                            sample.fail("EARLY FAIL")
                    return samples

                pre = pre_hook()(hook_factory("pre"))
                pre_always = pre_hook(condition="always")(hook_factory("pre_always"))
                pre_unprocessed = pre_hook(condition="unprocessed")(hook_factory("pre_unprocessed"))
                pre_failed = pre_hook(condition="failed")(hook_factory("pre_failed"))
                post_always = post_hook(condition="always")(hook_factory("post_always"))
                post_complete = post_hook(condition="complete")(hook_factory("post_complete"))
                post_failed = post_hook(condition="failed")(hook_factory("post_failed"))
                pre_per_runner = pre_hook(per="runner")(hook_factory("pre_per_runner"))
                post_per_runner_always = post_hook(per="runner", condition="always")(hook_factory("post_per_runner_always"))
                post_per_runner_complete = post_hook(per="runner", condition="complete")(hook_factory("post_per_runner_complete"))
                post_per_runner_fail  = post_hook(per="runner", condition="failed")(hook_factory("post_per_runner_failed"))
                post_per_sample_always = post_hook(per="sample", condition="always")(hook_factory("post_per_sample_always"))
                post_per_sample_complete  = post_hook(per="sample", condition="complete")(hook_factory("post_per_sample_complete"))
                post_per_sample_failed = post_hook(per="sample", condition="failed")(hook_factory("post_per_sample_failed"))

                @post_hook(after="all")
                def log_var(samples, logger, **_):
                    logger.info(f"SAMPLES VAR: {samples.var}")
                    for sample in samples:
                        logger.info(f"SAMPLE VAR - {sample.id}: {sample.var}")
                    return samples
            """,
            "samples.yaml": """
                - id: fail_early
                  files:
                  - input/fail.txt
                - id: pass_x_a
                  group: x
                  files:
                  - input/pass.txt
                - id: pass_x_b
                  group: x
                  files:
                  - input/pass.txt
                - id: pass_y_c
                  group: y
                  files:
                  - input/pass.txt
                - id: fail_x_a
                  group: x
                  files:
                  - input/fail.txt
                - id: fail_y_b
                  group: y
                  files:
                  - input/fail.txt
                - id: fail_y_c
                  group: y
                  files:
                  - input/fail.txt
            """,
            "input/pass.txt": "PASS",
            "input/fail.txt": "FAIL",
        },
    )
    def test_hooks(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Running pre hook",
            "Running pre_always hook",
            "Running pre_unprocessed hook",
            "Running pre_failed hook",
            "Running post_always hook",
            "Running post_failed hook",
            "Running post_complete hook",
            "Running pre_per_runner hook",
            "Running post_per_runner_always hook",
            "pre: ['fail_x_a', 'fail_y_b', 'fail_y_c', 'pass_x_a', 'pass_x_b', 'pass_y_c']",
            "pre_always: ['fail_early', 'fail_x_a', 'fail_y_b', 'fail_y_c', 'pass_x_a', 'pass_x_b', 'pass_y_c']",
            "pre_unprocessed: ['fail_x_a', 'fail_y_b', 'fail_y_c', 'pass_x_a', 'pass_x_b', 'pass_y_c']",
            "pre_failed: ['fail_early']",
            "pre_per_runner: ['fail_x_a', 'pass_x_a', 'pass_x_b']",
            "pre_per_runner: ['fail_y_b', 'fail_y_c', 'pass_y_c']",
            "post_per_runner_always: ['fail_x_a', 'pass_x_a', 'pass_x_b']",
            "post_per_runner_always: ['fail_y_b', 'fail_y_c', 'pass_y_c']",
            "post_per_runner_complete: ['pass_x_a', 'pass_x_b']",
            "post_per_runner_complete: ['pass_y_c']",
            "post_per_runner_failed: ['fail_x_a']",
            "post_per_runner_failed: ['fail_y_b', 'fail_y_c']",
            "post_per_sample_always: ['pass_x_a']",
            "post_per_sample_always: ['pass_x_b']",
            "post_per_sample_always: ['pass_y_c']",
            "post_per_sample_always: ['fail_x_a']",
            "post_per_sample_always: ['fail_y_b']",
            "post_per_sample_always: ['fail_y_c']",
            "post_per_sample_complete: ['pass_x_a']",
            "post_per_sample_complete: ['pass_x_b']",
            "post_per_sample_complete: ['pass_y_c']",
            "post_per_sample_failed: ['fail_x_a']",
            "post_per_sample_failed: ['fail_y_b']",
            "post_per_sample_failed: ['fail_y_c']",
            "post_failed: ['fail_early', 'fail_x_a', 'fail_y_b', 'fail_y_c']",
            "post_complete: ['pass_x_a', 'pass_x_b', 'pass_y_c']",
            "post_always: ['fail_early', 'fail_x_a', 'fail_y_b', 'fail_y_c', 'pass_x_a', 'pass_x_b', 'pass_y_c']",
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",

            "modules/a.py": """
                from cellophane import pre_hook
                @pre_hook(before="all")
                def setup_hook(samples, logger, **_):
                    samples[0].fail("DUMMY")
                    return samples

                @pre_hook()
                def default_pre_hook(samples, logger, **_):
                    logger.info(f"Default hook executed for {[s.id for s in samples]}")


                @pre_hook(condition="unprocessed")
                def unprocessed_pre_hook(samples, logger, **_):
                    logger.info(f"Unprocessed hook executed for {[s.id for s in samples]}")

                @pre_hook(condition="failed")
                def failed_pre_hook(samples, logger, **_):
                    logger.info(f"Failed hook executed for {[s.id for s in samples]}")
                @pre_hook(condition="always")
                def always_pre_hook(samples, logger, **_):
                    logger.info(f"Always hook executed for {[s.id for s in samples]}")
            """
        }
    )
    def test_conditional_pre_hook(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Default hook executed for ['b']",
            "Unprocessed hook executed for ['b']",
            "Failed hook executed for ['a']",
            "Always hook executed for ['a', 'b']",
        )

    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import exception_hook

                @exception_hook()
                def exception_hook_cellophane_main(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception}")
            """
        },
        mocks={
            "cellophane.cellophane._main": {
                "side_effect": Exception("DUMMY")
            }
        },
    )
    def test_exception_hook_cellophane_main(self, invocation: Invocation) -> None:
        assert invocation.logs == literal("Exception hook got exception: DUMMY")


    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, runner, Samples

                class SampleMixin(Samples):
                    foo: str = "foo"

                @Samples.merge.register("foo")
                def merge_foo(this, that):
                    raise Exception("DUMMY")

                @runner()
                def runner_a(samples, **_):
                    return samples

                @runner()
                def runner_b(samples, **_):
                    return samples

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_samples_merge(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            'Unhandled exception when merging samples: Exception(\'DUMMY\')',
            'Exception hook got exception: Exception(\'DUMMY\')'
        )


    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        mocks={
            "cellophane.cleanup.cleanup.Cleaner.__and__": {
                "side_effect": Exception("DUMMY")
            }
        },
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, runner

                @runner()
                def runner_a(samples, **_):
                    return samples

                @runner()
                def runner_b(samples, **_):
                    return samples

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_cleaner_merge(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            'Unhandled exception when merging cleaners: Exception(\'DUMMY\')',
            'Exception hook got exception: Exception(\'DUMMY\')'
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        mocks={
            "cellophane.modules.dispatcher.WorkerPool.apply_async": {
                "side_effect": Exception("DUMMY")
            }
        },
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
            """,
            "input/a.txt": "INPUT_A",
            "modules/a.py": """
                from cellophane import exception_hook, runner

                @runner()
                def runner_a(samples, **_):
                    return samples

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_start_runners(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            'Unhandled exception when starting runners: Exception(\'DUMMY\')',
            'Exception hook got exception: Exception(\'DUMMY\')'
        )


    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, runner

                @runner()
                def runner_(samples, **_):
                    raise Exception("DUMMY")

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_runner(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unhandled exception in runner 'runner_': Exception('DUMMY')",
            "Exception hook got exception: Exception('DUMMY')"
        )


    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, pre_hook

                @pre_hook()
                def pre_hook_(samples, **_):
                    raise Exception("DUMMY")

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_pre_hook(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unhandled exception in pre hook 'pre_hook_': Exception('DUMMY')",
            "Exception hook got exception: Exception('DUMMY')"
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, post_hook

                @post_hook()
                def post_hook_(samples, **_):
                    raise Exception("DUMMY")

                @exception_hook()
                def exception_hook_runner(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_post_hook(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unhandled exception in post hook 'post_hook_': Exception('DUMMY')",
            "Exception hook got exception: Exception('DUMMY')"
        )


    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, pre_hook

                @pre_hook()
                def pre_hook_(samples, **_):
                    raise Exception("PRE HOOK")

                @exception_hook()
                def exception_hook_(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
                    raise Exception("EXCEPTION HOOK")
            """
        }
    )
    def test_exception_hook_exception(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unhandled exception in pre hook 'pre_hook_': Exception('PRE HOOK')",
            "Exception hook got exception: Exception('PRE HOOK')",
            "Unhandled exception in exception hook 'exception_hook_': Exception('EXCEPTION HOOK')",
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
                - id: b
                  files:
                  - input/b.txt
            """,
            "input/a.txt": "INPUT_A",
            "input/b.txt": "INPUT_B",
            "modules/a.py": """
                from cellophane import exception_hook, pre_hook

                @pre_hook()
                def pre_hook_(samples, executor, **_):
                    executor.submit("INVALID", wait=True, name="DUMMY")

                @exception_hook()
                def exception_hook_(exception, logger, **_):
                    logger.info(f"Exception hook got exception: {exception!r}")
            """
        }
    )
    def test_exception_hook_executor(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unhandled exception in subprocess job 'DUMMY'",
            "Exception hook got exception: SystemExit(1)",
        )


    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import pre_hook, Samples

                class TestSamples(Samples):
                    executed = set()

                    @property
                    def not_executed(self):
                        return {
                            "a",
                            "before_all_a",
                            "before_all_after_a",
                            "before_x",
                            "before_xy",
                            "x",
                            "y",
                            "after_x_before_y",
                            "after_x",
                            "after_xy",
                            "after_all_before_b",
                            "after_all_b",
                            "b",
                        } - self.executed

                def hook_info(samples, msg=None):
                    return "".join(
                        (
                            (f"{msg}\\n" if msg else ""),
                            f"Executed: {', '.join(samples.executed)}\\n",
                            f"Not executed: {', '.join(samples.not_executed)}\\n",
                        )
                    )

                @pre_hook(before="all")
                def a(samples, logger, **_):
                    assert "before_all_a" in samples.executed, hook_info(samples, msg="Expected before_all_a to be executed")
                    assert "before_all_after_a" in samples.not_executed, hook_info(samples, msg="Expected before_all_after_a to not be executed")
                    samples.executed.add("a")

                @pre_hook(before=["all", "a"])
                def before_all_a(samples, logger, **_):
                    assert not samples.executed, hook_info(samples, msg="Expected no hooks to be executed")
                    samples.executed.add("before_all_a")

                @pre_hook(before="all", after="a")
                def before_all_after_a(samples, logger, **_):
                    assert "a" in samples.executed, hook_info(samples, msg="Expected 'a' to be executed")
                    samples.executed.add("before_all_after_a")

                @pre_hook(before="x")
                def before_x(samples, logger, **_):
                    assert "x" in samples.not_executed, hook_info(samples, msg="Expected 'x' to not be executed")
                    samples.executed.add("before_x")

                @pre_hook(before=["x", "y"])
                def before_xy(samples, logger, **_):
                    samples.executed.add("before_xy")

                @pre_hook()
                def x(samples, logger, **_):
                    samples.executed.add("x")

                @pre_hook()
                def y(samples, logger, **_):
                    samples.executed.add("y")

                @pre_hook(after="x", before="y")
                def after_x_before_y(samples, logger, **_):
                    assert "x" in samples.executed, hook_info(samples, msg="Expected 'x' to be executed")
                    assert "y" not in samples.executed, hook_info(samples, msg="Expected 'y' to not be executed")
                    samples.executed.add("after_x_before_y")

                @pre_hook(after="x")
                def after_x(samples, logger, **_):
                    assert "x" in samples.executed, hook_info(samples, msg="Expected 'x' to be executed")
                    samples.executed.add("after_x")

                @pre_hook(after=["x", "y"])
                def after_xy(samples, logger, **_):
                    assert {"x", "y"} <= samples.executed, hook_info(samples, msg="Expected 'x' and 'y' to be executed")
                    samples.executed.add("after_xy")

                @pre_hook(after="all", before="b")
                def after_all_before_b(samples, logger, **_):
                    assert "b" not in samples.executed, hook_info(samples, msg="Expected 'b' to not be executed")
                    samples.executed.add("after_all_before_b")

                @pre_hook(after=["all", "b"])
                def after_all_b(samples, logger, **_):
                    assert "db" in samples.executed, hook_info(samples, msg="Expected 'b' to be executed")
                    samples.executed.add("after_all_b")

                @pre_hook(after="all")
                def b(samples, logger, **_):
                    samples.executed.add("b")
            """,
        },
    )
    def test_hooks_order(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Running a hook",
            "Running before_all_a hook",
            "Running before_all_after_a hook",
            "Running before_x hook",
            "Running before_xy hook",
            "Running x hook",
            "Running after_x_before_y hook",
            "Running after_x hook",
            "Running after_xy hook",
            "Running after_all_before_b hook",
            "Running after_all_b hook",
            "Running b hook",
        )

    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import pre_hook

                class INVALID: ...

                @pre_hook(before=INVALID)
                def a(samples, logger, **_):
                    ...
            """
        },
    )
    def test_invalid_hook_before(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unable to import module 'a': ValueError(\"a: before=<class 'modules.a.INVALID'>, after=[]\")"
        )

    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import pre_hook

                class INVALID: ...

                @pre_hook(after=INVALID)
                def a(samples, logger, **_):
                    ...
            """
        },
    )
    def test_invalid_hook_after(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unable to import module 'a': ValueError(\"a: before=[], after=<class 'modules.a.INVALID'>\")"
        )

    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import pre_hook

                class INVALID: ...

                @pre_hook()
                def return_samples(samples, logger, **_):
                    return samples

                @pre_hook()
                def return_none(samples, logger, **_):
                    ...

                @pre_hook()
                def return_invalid(samples, logger, **_):
                    return INVALID()
            """
        },
    )
    def test_hook_return(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unexpected return type <class 'modules.a.INVALID'>",
            "Hook did not return any samples",
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "modules/a.py": """
                from cellophane import pre_hook

                @pre_hook()
                def a(samples, logger, **_):
                    raise KeyboardInterrupt()
            """,
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
            """,
            "input/a.txt": "INPUT_A",
        },
    )
    def test_hook_keyboard_interrupt(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Keyboard interrupt received, failing samples and stopping execution"
        )

    @mark.override(
        args=[*args, "--samples_file samples.yaml"],
        structure={
            "modules/a.py": """
                from cellophane import pre_hook

                @pre_hook()
                def a(samples, logger, **_):
                    raise Exception("BOOM")
            """,
            "samples.yaml": """
                - id: a
                  files:
                  - input/a.txt
            """,
            "input/a.txt": "INPUT_A",
        },
    )
    def test_hook_exception(self, invocation: Invocation) -> None:
        assert invocation.logs == literal("Unhandled exception in pre hook 'a': Exception('BOOM')")

    @mark.override(
        structure={
            "modules/a.py": """
                from cellophane import post_hook

                @post_hook(condition="INVALID")
                def a(**_): ...
            """
        },
    )
    def test_hook_invalid_condition(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "Unable to import module 'a': ValueError(\"condition='INVALID' must be one of 'always', 'complete', 'failed'\")"
        )
