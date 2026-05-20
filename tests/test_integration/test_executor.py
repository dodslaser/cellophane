from pytest import mark

from cellophane.testing import BaseTest, Invocation, literal, regex

UUID4_REGEX = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
UUID4_HEX_REGEX = r"[0-9a-f]{32}"
UUID_SHORT_REGEX = r"[0-9a-f]{8}"
TIMESTAMP_REGEX = r"[0-9]{6}-[0-9]{6}"
PID_REGEX = r"[0-9]{1,5}"

class Test_executor_submit(BaseTest):
    args = [
        "--samples_file samples.yaml",
        "--workdir out",
    ]

    structure = {
        "samples.yaml": """
            - id: a
              files:
              - input/a.txt
        """,
        "input/a.txt": "INPUT_A",
    }

    @mark.override(
        args=[*args, "--executor_name mock"],
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook, runner, Executor, executors

                @pre_hook()
                def hook_(logger, samples, executor, **_):
                    logger.info(f"HOOK - {executor.name=}")
                    logger.info(f"HOOK - {executors.EXECUTOR.name=}")
                    executor.submit("ping localhost -c 1", wait=True, name="HOOK")
                    return samples

                @runner()
                def runner_(logger, samples, executor, **_):
                    logger.info(f"RUNNER - {executor.name=}")
                    logger.info(f"RUNNER - {executors.EXECUTOR.name=}")
                    executor.submit("ping localhost -c 1", wait=True, name="RUNNER")
                    return samples
            """,
        },
    )
    def test_executor_submit(self, invocation: Invocation) -> None:
        assert invocation.logs == literal(
            "HOOK - executor.name='mock'",
            "HOOK - executors.EXECUTOR.name='mock'",
            "RUNNER - executor.name='mock'",
            "RUNNER - executors.EXECUTOR.name='mock'",
            "MockExecutor called with name='RUNNER'",
            "MockExecutor called with name='HOOK'",
            "cmdline=ping localhost -c 1",
            "os_env=True",
            "cpus=1",
            "memory=2000000000",
        )
        assert invocation.logs == regex(
            f"uuid={UUID4_REGEX}",
            f"workdir=out/{TIMESTAMP_REGEX}/runner_/RUNNER\\.{UUID4_HEX_REGEX}\\.mock",
            f"workdir=out/{TIMESTAMP_REGEX}/HOOK\\.{UUID4_HEX_REGEX}\\.mock",
        )

    @mark.override(
        args=[*args, "--executor_name subprocess"],
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook

                @pre_hook()
                def runner_a(logger, samples, executor, config, **_):
                    result, uuid = executor.submit("ping localhost -c 1", wait=True)
            """,
        },
    )
    def test_subprocess_executor(self, invocation: Invocation) -> None:
        assert invocation.logs == regex(
            "Using subprocess executor",
            f"Starting subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Started process \\(pid={PID_REGEX}\\) for job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Completed subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
        )

    @mark.override(
        args=[*args, "--executor_name subprocess"],
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook
                from time import sleep

                @pre_hook()
                def runner_a(logger, samples, executor, config, **_):
                    executor.submit("ping localhost -c 2")
                    executor.terminate()
                    executor.wait()
            """,
        },
    )
    def test_subprocess_executor_terminate(self, invocation: Invocation) -> None:
        assert invocation.logs == regex(
            f"Started process \\(pid={PID_REGEX}\\) for job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Starting subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Started process \\(pid={PID_REGEX}\\) for job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Terminating subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Terminating process \\(pid={PID_REGEX}\\)",
            f"Process \\(pid={PID_REGEX}\\) exited with code -15",
            f"Error in subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
        )

    @mark.override(
        args=[*args, "--executor_name mock"],
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook, Executor, runner

                @runner()
                def runner_a(executor, **_):
                    result, uuid = executor.submit(
                        "ping localhost -c 1",
                        wait=True,
                        conda_spec={"dependencies": ["python >=3.8,<3.9"]},
                    )

                @runner()
                def runner_b(executor, **_):
                    result, uuid = executor.submit(
                        "ping localhost -c 1",
                        wait=True,
                        conda_spec={"dependencies": ["python >=3.8,<3.9"]},
                    )
            """,
        },
    )
    def test_executor_conda(self, invocation: Invocation) -> None:
        assert invocation.logs == regex(
            "Built environment at .+/\\.environments/conda_[0-9a-f]{8}",
            "Environment already exists at .+/\\.environments/conda_[0-9a-f]{8}",
            f"env.PATH=[^:]+/out/{TIMESTAMP_REGEX}/runner_a/mock_job\\.{UUID4_HEX_REGEX}\\.mock/conda_[0-9a-f]{{8}}/bin:.*",
            f"env.PATH=[^:]+/out/{TIMESTAMP_REGEX}/runner_b/mock_job\\.{UUID4_HEX_REGEX}\\.mock/conda_[0-9a-f]{{8}}/bin:.*",
            "env\\.CONDA_SHLVL=1",
            "env\\.CONDA_PREFIX=conda_[0-9a-f]{8}",
        )


    @mark.override(
        args=[*args, "--executor_name subprocess"],
        subprocess_mocks={"ping localhost -c 1": {"returncode": 1337}},
        mocks={
            "cellophane.executors.subprocess_executor.psutil.Process": {},
            "cellophane.executors.subprocess_executor.psutil.Process.pid": {
                "new": "42"
            },
            "cellophane.executors.subprocess_executor.psutil.Process.wait": {
                "return_value": 1337
            },
        },
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook, Executor

                @pre_hook()
                def runner_a(executor, **_):
                    result, uuid = executor.submit("ping localhost -c 1", wait=True)
            """,
        },
    )
    def test_subprocess_executor_command_non_zero(self, invocation: Invocation) -> None:
        assert invocation.logs == regex(
            f"Starting subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Started process \\(pid=1\\) for job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Non-zero exit code \\(1337\\) for subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
            f"Error in subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
        )

    @mark.override(
        args=[*args, "--executor_name subprocess"],
        mocks={
            "cellophane.executors.subprocess_executor.sp.Popen": {
                "side_effect": Exception("BOOM")
            },
        },
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook, Executor

                @pre_hook()
                def runner_a(executor, **_):
                    result, uuid = executor.submit("ping localhost -c 1")
            """,
        },
    )
    def test_subprocess_executor_command_exception(
        self, invocation: Invocation
    ) -> None:
        assert invocation.logs == regex(
            f"Unhandled exception in subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\): Exception\\('BOOM'\\)",
            f"Error in subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
        )

    @mark.override(
        args=[*args, "--executor_name subprocess"],
        mocks={
            "cellophane.executors.subprocess_executor.sp.Popen": {
                "side_effect": Exception("BOOM")
            },
        },
        structure={
            **structure,
            "modules/a.py": """
                from cellophane import pre_hook, Executor
                def _callback(*args, **kwargs):
                    raise Exception("BOOM")

                @pre_hook()
                def runner_a(executor, **_):
                    result, uuid = executor.submit("ping localhost -c 1", error_callback=_callback)
            """,
        },
    )
    def test_subprocess_executor_callback_exception(
        self, invocation: Invocation
    ) -> None:
        assert invocation.logs == regex(
            f"Unhandled exception in callback of subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\): Exception\\('BOOM'\\)",
            f"Error in subprocess job 'subprocess_job' \\(UUID={UUID_SHORT_REGEX}\\)",
        )
