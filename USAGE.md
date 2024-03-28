<!-- markdownlint-disable MD033 Used for spoilers -->
<!-- markdownlint-disable MD028 Used as notes -->
<!-- markdownlint-disable MD013 Allow long lines -->

# Usage

## Dependencies

At the very least wou will need an environment with python 3.11+ and pip installed. In this example micromamba is used to create a new environment.

```shell
micromamba env create -p my_cellphane_env python=3.11
micromamba activate my_cellphane_env
pip install git+https://github.com/ClinicalGenomicsGBG/cellophane.git@latest
```

## Initializing a wrapper

The `cellophane init` command will initialize a new wrapper in the current directory. This command does the following:

- Set up the directory structure for the wrapper.
- Add an empty `schema.yaml` file where you can define configuration options.
- Create `config.example.yaml` describing the configuration options.
- Create a `requirements.txt` file with the required python packages.
- Create `__main__.py` and `<WRAPPER>.py` files as entrypoints for the wrapper.
- Create a git repository and add a `.gitignore` file (you will need to add a remote manually).

```text
mkdir my_awesome_wrapper
cd my_awesome_wrapper

python -m cellophane init my_awesome_wrapper
```

You should end up with this directory structure.

```text
./my_awesome_wrapper/
├── __init__.py
│
│   # Directory containing cellophane modules
├── modules
│
│   # Directory containing scripts to be submitted by Popen, SGE, etc.
├── scripts
│
│   # JSON Schema defining configuration options
├── schema.yaml
│
│   # Example configuration file
├── config.example.yaml
│
│   # Additional python packages required by the wrapper
├── requirements.txt
│
│   # Main entrypoint for the wrapper
└── __main__.py
│
│   # Alternative entrypoint for the wrapper
└── my_awesome_wrapper.py
```

## Defining a wrapper

All cellophane wrapper code is be defined inside one or more module(s). A module can be a `.py` file, or a directory containing a `__init__.py` file, placed in the `modules` directory. Cellophane will automatically import all modules, and make them available to the wrapper.

A cellophane wrapper is comprised of pre-hooks, post-hooks, and runners. Samples are passed sequentially trough the pre-hooks, then passed to all runners in parallel, merged, and passed sequentially trough the post-hooks.

Samples are passed to the hooks and runners as a `Samples` object. This object is essentially a list of `Sample` objects. Each `Sample` object represents one sample, and contains a set of attributes describing the sample. The `Samples` object also contains a set of attributes describing the samples as a whole.

In addition to the hooks and runners, a module can also define mixin classes by subclassing `Sample` and/or `Samples`. These mixins can be used to add attributes and methods to the samples.

See below for a more detailed description of the different parts of a cellophane wrapper.

<details>

---

<summary><strong>Pre-hooks</strong></summary>

Pre-hooks are used to prepare samples for the runners. They are executed sequentially, and can be used to create input files, set up databases, etc. Pre-hooks can also add new samples to the sample list, or remove samples from the sample list.

Pre-hooks are defined by decorating a function with `@cellophane.pre_hook()`. The decorator takes the following arguments:

Argument      | Type              | Description
--------------|-------------------|-------------
`label`       | `str`             | A label for the pre-hook to use in logs. If not specified, the function name will be used. Note that the hook name (used in `before`/`after`) will always be the same as the function name.
`before`      | `str\|list[str]` | A name or list of names specifying which pre-hooks this pre-hook will run before. If `before` is set to `"all"`, the pre-hook will run before all other pre-hooks.
`after`       | `str\|list[str]` | A name or list of names specifying which pre-hooks will run before this pre-hook. If `after` is set to `"all"`, the pre-hook will run after all other pre-hooks.
---

At runtime, the decorated function (hook) will be called with the following keyword arguments:

Argument    | Type                    | Description
------------|-------------------------|-------------
`samples`   | `cellophane.Samples`    | Samples to process.
`config`    | `cellophane.Config`     | Wrapper configuration.
`timestamp` | `str`                   | A string representation of the current timestamp (YYYYMMDDHHMMSS).
`logger`    | `logging.LoggerAdapter` | A logger that can be used to log messages.
`root`      | `pathlib.Path`          | A `pathlib.Path` pointing to the root directory of the wrapper repository.
`workdir`   | `pathlib.Path`          | A `pathlib.Path` pointing to the working directory of the hook.
`executor`  | `cellophane.Executor`   | An `Executor` that can be used to run external commands.

---

</details>

<details>

---

<summary><strong>Runners</strong></summary>

Runners are used to execute the main task of the wrapper (eg. an external pipeline). They are executed in parallel, and the output of one runner should not depend on the output of another runner.

Runners are defined by decorating a function with `@cellophane.runner()`. The decorator takes the following arguments:

Argument      | Type        | Description
--------------|-------------|-------------
`label`       | `str`       | A label for the pre-hook to use in logs. If not specified, the function name will be used. Note that the hook name (used in `before`/`after`) will always be the same as the function name.
`split_by`    | `str`       | An attribute name to split the samples by. If specified, the runner will be called with subsets of the samples, where each subset contains samples with the same value for the specified attribute.
---

At runtime, the decorated function (runner) will be called with the following keyword arguments:

Argument    | Type                    | Description
------------|-------------------------|-------------
`samples`   | `cellophane.Samples`    | Samples to process.
`config`    | `cellophane.Config`     | Wrapper configuration.
`timestamp` | `str`                   | A string representation of the current timestamp (YYYYMMDDHHMMSS).
`logger`    | `logging.LoggerAdapter` | A logger that can be used to log messages.
`root`      | `pathlib.Path`          | A `pathlib.Path` pointing to the root directory of the wrapper.
`workdir`   | `pathlib.Path`          | A `pathlib.Path` pointing to the working directory of the runner.
`executor`  | `cellophane.Executor`   | An `Executor` that can be used to run external commands.

---

</details>

<details>

---

<summary><strong>Post-hooks</strong></summary>

Post-hooks are used to process the output of the runners. They are executed sequentially, and can be used to move output files, clean up databases, etc. Post-hooks should generally not add or remove samples from the sample list, but it is possible to do so.

Post-hooks are defined by decorating a function with `@cellophane.post_hook()`. The decorator takes the following arguments:

Argument      | Type              | Description
--------------|-------------------|-------------
`label`       | `str`             | A label for the pre-hook to use in logs. If not specified, the function name will be used. Note that the hook name (used in `before`/`after`) will always be the same as the function name.
`before`      | `str\|list[str]` | A name or list of names specifying which pre-hooks this pre-hook will run before. If `before` is set to `"all"`, the pre-hook will run before all other pre-hooks.
`after`       | `str\|list[str]` | A name or list of names specifying which pre-hooks will run before this pre-hook. If `after` is set to `"all"`, the pre-hook will run after all other pre-hooks.
---

At runtime, the decorated function (hook) will be called with the following keyword arguments:

Argument    | Type                    | Description
------------|-------------------------|-------------
`samples`   | `cellophane.Samples`    | Samples to process.
`config`    | `cellophane.Config`     | Wrapper configuration.
`timestamp` | `str`                   | A string representation of the current timestamp (YYYYMMDDHHMMSS).
`logger`    | `logging.LoggerAdapter` | A logger that can be used to log messages.
`root`      | `pathlib.Path`          | A `pathlib.Path` pointing to the root directory of the wrapper repository.
`workdir`   | `pathlib.Path`          | A `pathlib.Path` pointing to the working directory of the hook.
`executor`  | `cellophane.Executor`   | An `Executor` that can be used to run external commands.

---

</details>

<details>

---

<summary><strong>Sample(s)</strong></summary>

By default, the `data.Sample` class has the following attributes:

Attribute    | Type                            | Description
-------------|---------------------------------|-------------
`uuid`         | `uuid.UUID`                   | An automatically assigned unique identifier for the sample.
`id`           | `str`                         | A potentially non-unique user-defined identifier for the sample.
`files`        | `list[pathlib.Path]`          | A list of paths to files associated with the sample.
`processed`    | `bool`                        | A boolean indicating if the sample has been processed.
`meta`         | `cellophane.data.Container`   | A mapping containing user-defined metadata for the sample.
`failed`       | `str (property)`              | A string describing why the sample failed processing.
`merge`        | `ClassVar[data._Merger]`      | A class used to register merge functions for sample attributes (Used when defining mixins).
`fail`         | `Callable`                    | A function used to set the `failed` attribute.
`with_mixins`  | `Callable`                    | A function used to create a new sample with additional mixins (Used internally, rarely needed).
---
The `data.Samples` class has the following attributes:

Attribute           | Type                                            | Description
--------------------|-------------------------------------------------|-------------
`data`              | `list[cellophane.Sample]`                       | The list of samples (Used internally, rarely accessed directly).
`sample_class`      | `ClassVar[type[cellophane.Sample]]`             | The class used to create new samples (Used internally, rarely accessed)
`merge`             | `ClassVar[cellophane.data._Merger]`             | A class used to register merge functions for sample attributes (Used when defining mixins).
`output`            | `set[cellophane.Output\|cellophane.OutputGlob]` | A set of output files and/or globs. Globs are expanded to a set of files before the samples are passed to the post-hooks
`from_file`         | `Callable`                                      | A function used to create a new `Samples` object from a `samples.yaml` file.
`with_mixins`       | `Callable`                                      | A function used to create a new `Samples` object with additional mixins (Used internally, rarely accessed).
`with_sample_class` | `Callable`                                      | A function used to create a new `Samples` object with a different sample class (Used internally, rarely accessed).
`split`             | `Callable`                                      | A function used to split the samples into subsets based on an attribute. By default `uuid` is used as the attribute, splitting the samples into subsets of one sample each.
`unique_ids`        | `str (property)`                                | A function used to get a set of unique sample IDs.
`with_files`        | `cellophane.Samples (property)`                 | A function used to create a new `Samples` object with only samples that have files.
`without_files`     | `cellophane.Samples (property)`                 | A function used to create a new `Samples` object with only samples that do not have files.
`complete`          | `cellophane.Samples (property)`                 | A function used to create a new `Samples` object with only samples that have been processed by all runners nad not explicitly or implicitly failed.
`failed`            | `cellophane.Samples (property)`                 | A function used to create a new `Samples` object with only samples that have explicitly or implicitly failed for at least one runner.
---

Mixins are used to add attributes and methods to the `Samples` and `Sample` classes. They are defined by creating a class that inherits from `Samples` or `Sample`. The classes use `attrs` under the hood, so all `attrs` features are available (eg. validators, on_setattr, fields, etc.).

> **Note:** Mixin classes MUST NOT be slotted, as this will break mixin merging. If the `attrs.define` decorator is used, the `slots` argument must be set to `False`.

> **Note:** Mixin classes must specify default values for added attributes. If no default value is desired, use `None` as a sentinel value.

When samples are collected from runners, any inconsistencies between multiple instances of the same sample must be resolved. In cellophane, this is known as merging. The merge attribute allows you to register merge functions for sample attributes. Merge functions take in two values and return a combined value. If no merge function is registered for an attribute, all the values will be returned in a tuple. Merge functions are repeated for each instance of the value, with the merged value passed on to the next iteration.

---

</details>

<details>

---

<summary><strong>Executors</strong></summary>

Executors are used to run external commands (eg. as a subprocess, via DRMAA, etc.). All executors share the same interface, and can be used interchangeably. An instance of the active executor is passed to the hooks and runners as a keyword argument. For most usecases, the `submit`, `wait`, and possibly `terminate` methods are the only ones that need to be used.

> **Note:** Each runner/hook gets a separate executor instance. Only jobs submitted in the current hook/runner can be waited for/terminated.

```python
Executror.submit(
    name = __name__,
    wait = False,
    uuid = None,
    workdir = None,
    env = None,
    os_env = True,
    callback = None,
    error_callback = None,
    cpus = None,
    memory = None,
) -> tuple[AsyncResult, UUID]
```

Executes a command. Returns a tuple containing an `mpire.AsyncResult` object and the `uuid.UUID` identifying the executed command.

Argument        | Type                    | Description
----------------|-------------------------|------------
`name`          | `str`                   | A name for the command. This will be used in logs.
`wait`          | `bool`                  | If `True`, block until the command finishes.
`uuid`          | `uuid.UUID`             | A unique identifier for the command. If not specified, a new UUID will be generated.
`workdir`       | `pathlib.Path`          | A `pathlib.Path` pointing to the working directory of the command. If not specified, the current working directory will be used.
`env`           | `dict`                  | A dictionary of environment variables to set for the command.
`os_env`        | `bool`                  | If `True`, the current environment variables will be passed to the command.
`callback`      | `Callable`              | A function to call when the command finishes successfully. `None` will be passed as the only argument.
`error_callback`| `Callable`              | A function to call when the command fails. The exception will be passed as the only argument.ß
`cpus`          | `int`                   | The number of CPUs to request for the command (not used by all executors).
`memory`        | `int`                   | The amount of memory to request for the command (not used by all executors).
---

```python
Executror.wait(uuid = None) -> None
```

Waits for submitted commands to finish.

Argument        | Type                    | Description
----------------|-------------------------|------------
`uuid`          | `uuid.UUID`             | A unique identifier for the command. If not specified, all submitted commands will be waited for.
---

```python
Executror.terminate(uuid = None) -> None
```

Terminates submitted commands.

Argument        | Type                    | Description
----------------|-------------------------|------------
`uuid`          | `uuid.UUID`             | A unique identifier for the command. If not specified, all submitted commands will be terminated.
---

</details>

## Example module

```python
# modules/my_module.py

import attrs
import logging
import pathlib

from cellophane import pre_hook, runner, post_hook, Sample, Samples, Config, Executor

# Mixins can be defined as simple classes...
class MySample(Sample):
    # Add a new attribute
    my_attribute: str | None = None

# ...or more advanced using attrs
@attrs.define(slots=False)
class MySamples(Samples):
    # Add a new field using attrs
    my_field: str | None = attrs.field(default=None, on_setattr=attrs.setters.validate)

    # attrs features can be used as normal
    @my_field.validator
    def _validate_my_field(self, attribute, value):
        if value == "nope":
            raise ValueError("my_field cannot be 'nope'")

@pre_hook(
    label="Some hook label",
    before="all"
)
def my_pre_hook(
    samples: Samples,
    config: Config,
    logger: logging.LoggerAdapter,
    **_,
) -> Samples:
    logger.debug(config.max_file_size)
    for sample in samples.copy():
        # Example hook that removes samples with too large files
        if any(
            # max_file_size should be defined in schema.yaml
            file.stat().st_size > config.max_file_size
            for file in sample.files
        ):
            logger.warning(f"Sample {sample.id} has a file larger than {config.max_file_size} bytes")
            samples.remove(sample)

    return samples

@runner(
    label="My runner",
    split_by="my_attribute"
)
def my_runner(
    samples: Samples,
    config: Config,
    logger: logging.LoggerAdapter,
    workdir: pathlib.Path,
    executor: Executor,
    root: pathlib.Path,
    **_,
) -> None:  # Runners may return None if they do not modify the samples

    # Execute a script for each sample in parallel using the executor
    for sample in samples:
        executor.submit(
            root / "scripts" / "my_script.sh",
            "--some-argument",
            workdir=workdir,
            env={
                "FILE1": sample.files[0],
                "FILE2": sample.files[1],
            },
            wait=False,
        )

    # Wait for all submitted scripts to finish
    executor.wait()

@post_hook(
    after="my_post_hook_b"
)
def my_post_hook_a(
    samples: Samples,
    config: Config,
    logger: logging.LoggerAdapter,
    workdir: pathlib.Path,
    **_,
) -> None:  # Hooks may also return None if they do not modify the samples
    for sample in samples:
        if sample.my_attribute is not None:
            # my_post_hook_a is set to run after my_post_hook_b,
            # so we can safely assume that my_attribute is set
            logger.info(f"Woah! A message from {sample.id}: {sample.my_attribute}")

@post_hook()
def my_post_hook_b(
    samples: Samples,
    config: Config,
    logger: logging.LoggerAdapter,
    workdir: pathlib.Path,
    **_,
) -> Samples:
    for sample in samples:
        logger.info(f"Setting my_attribute for sample {sample.id}")
        sample.my_attribute = f"Hello from {sample.id}!"
    return samples
```

# External modules

Cellophane includes a CLI to add/remove/update external modules. The CLI requires a clean git repository to work, and will not run otherwise. Any changes need to be committed, discarded, or stashed before adding, updating, or removing modules.

```text
python -m cellophane module add
```

This command will ask what module(s) to add and then what version to use for each module. The module(s) will be installed in the `modules` directory as a git submodule. The `config.example.yaml` and `requirements.txt` will be automatically updated. Finally, a git commit will be created with the changes.

Alternatively, you can select a module and version directly from the command-line. The `latest` tag will select the latest version of the module. The `dev` tag will select the `dev` branch of the module.

> **NOTE:** The `add`/`rm`/`update` commands can also be used without specifying a module. In this case, you will be prompted to select a module from a list of available modules.

```text
python -m cellophane module add slims@latest
```

After adding a module it is important to also install any dependencies required by the module. The `requirements.txt` file at the project root includes dependencies for all modules.

```text
pip install -r requirements.txt
```

To update a module to a specific version, use the `update` command. If a module is checked out at the `dev` branch, the `update` command will pull the latest changes from the remote repository.

```text
python -m cellophane module update slims@dev
```

To remove a module, use the `rm` command. This essentially does the reverse of the `add` command and creates a git commit with the changes.

```text
python -m cellophane module rm slims
```

# Configuration

Cellophane uses [JSON Schema](https://json-schema.org/) to define configuration options. Wrapper specific configuration options can be defined in `schema.yaml`. The schema will be merged with base schema (`schema.base.yaml`), as well as any schemas defined in modules. A CLI will be generated from the schema, and used to parse the configuration at runtime. The configuration will be passed to the runners and hooks, via the config keyword argument, as a `Config` object. This object allows for normal dict-like access with string keys (e.g. `config["bingo"]["bango"]`) or tuple keyes (e.g. `config["bingo", "bango"]`). It also allows for attribute access (e.g. `config.bingo.bango`).

> **NOTE:** Using `__dunder__` keys (eg. `config['__bar__'] = 'foo'`) is considered as unsupported behaviour.

> **NOTE:** The `Config` object implements the `Mapping` protocol, so stuff like `**unpacking` works at any level. *HOWEVER* attribute access to does not work for keys that overlab with a `dict` method (e.g. `keys`, `items`, `values`, etc.). In these cases, use `config["keys"]` instead of `config.keys`.

> **NOTE:** The `cellophane.data.to_dict` function can be used to convert a `Config` object to a nested dict.

An example configuration file is generated automatically when the wrapper is initialized, and updated automatically when modules are added/removed. It is also possible to use `--help` to list all available options and their descriptions. Note that if `--help` is specified along with other flags, the schema will be evaluated using the current configuration and flags. This means that the help text will only show options that are still required given the current configuration and flags.

Cellophane aims to be JSON Schema Draft 7 compliant, but complex schemas may break parsing. Please open an issue if you encounter any problems. These types are implemented in addition to the standard [JSON Schema types](https://json-schema.org/understanding-json-schema/reference/type.html):

- `path` - Used to specify that a string should be converted to a `pathlib.Path` object.
- `size` - Used to specify that a string should be converted to bytes (eg. `1G` -> `1000000000`, `10 MiB` -> `10485760`).
- `mapping` - Used to denote JSON objects that will be parsed when specified as a string (eg. `foo=bar,baz=qux` will be parsed as `{"foo": "bar", "baz": "qux"}`).

[Conditional validation](https://json-schema.org/understanding-json-schema/reference/conditionals.html) is supported but has a few caveats:

- `allOf` will simply combine the schemas without validating against current parameters.
- `oneOf` will select the *FIRST* schema that validates against current parameters.
- `anyOf` cannot be used to mark parameters as required, since validation will fail if the parameter is not present.

> **NOTE:** It is possible to "hide" options using conditionals that define properties absent from the base schema (e.g. only expose flag `--a` if flag `--b` is set to `foo`). This should be used with caution as it will be confusing to users.

## Example schema

```yaml
# schema.yaml

# Required options are marked with an asterisk in the help text.
required:
  - bingo
  - max_file_size

# Dependent required options will be marked as required if the specified option is set
dependentRequired:
  bongo:
    - foo

# if-then-else can be used to specify conditional validation
if:
  # NOTE: If 'required' is not specified the if-schema will evaluate to 'true' even if 'bongo' is not present in the config
  required:
    - bongo
  properties:
    bongo:
      const: bar
then:
  required:
    - bar
else:
  required:
    - baz

properties:
  bingo:
    # The 'object' type is used to nest options
    type: object
    # Nested required options will be marked if the parent is required or if the parent node is present in the config
    required:
      - bango
    properties:
      # When a nested option is converted to a CLI flag, the levels will be separated by underscores (eg. --bingo_bango)
      bango:
        # The 'type' keyword will be used to convert the value to the correct type
        type: string
        # The 'description' keyword will be used to generate the help text
        description: Some string

  bongo:
    type: string
    # The 'enum' keyword can be used to specify a list of allowed values
    enum:
      - foo
      - bar
    description: A string with a limited set of allowed values

  foo:
    type: array
    # Arrays can specify the type of their items
    items:
      type: integer
    description: A list of integers

  bar:
    type: mapping
    # The 'mapping' type is used to interpret a nested object as a mapping
    # When specified in the CLI, the mapping will be parsed from a string (eg. foo=bar,baz=qux)
    description: A mapping
    # The 'secret' keyword can be used to hide the value of an option in the help text
    secret: true

  baz:
    # Boolean options will be converted to flags (eg. --baz/--no_baz)
    type: boolean
    description: A boolean

  max_file_size:
    # The 'size' type is used to convert a string to bytes
    type: size
    description: The maximum file size in bytes
    # If a required option has a default value, it will not be marked as required in the help text
    default: "100 MiB"
```

# Running a wrapper

Wrappers can be run as a module using the `python -m my_awesome_wrapper` command, or using `python my_awesome_wrapper.py`. The `--config_file` parameter can be used to specify a YAML configuration file. The `--help` flag can be used to list all available options and their descriptions.

```yaml
# config.yaml
bongo: foo

foo:
- 13
# Values will be converted to the correct type (or die trying)
- "37"

# The 'mapping' type is used to interpret a nested object as a mapping
bar:
  some_nested_key: some_value
  another_nested_key: another_value
  we:
    have:
      to:
        go:
          deeper: "to get to the bottom of this"


```

> **Note:** CLI flags will override values specified in the configuration file.

> **Note:** If `--help` is specified along with other flags, the schema will be evaluated using the current configuration and flags. This means that the help text will only show options that are still required given the current configuration and flags.

```text
$ python -m my_awesome_wrapper --config_file config.yaml  --bongo bar --help

Usage: my_awesome_wrapper [OPTIONS]

╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│    --log_level          [DEBUG|INFO|WARNING|ERROR|CRITICAL]  Log level (INFO)                                                                     │
│    --executor_name      [subprocess]                         Name of the executor to use (subprocess)                                             │
│    --executor_cpus      INTEGER                              Number of CPUs to allocate to jobs started (if supported by the executor) (1)        │
│    --executor_memory    SIZE                                 Ammount of memory to allocate to jobs started (if supported by the executor) (2 GB)  │
│    --config_file        PATH                                 Path to config file                                                                  │
│    --logdir             PATH                                 Log directory (out/logs)                                                             │
│    --workdir            PATH                                 Working directory where intermediate files are stored (out)                          │
│    --resultdir          PATH                                 Results base directory where output files are copied (out/results)                   │
│    --tag                TEXT                                 Tag identifying the pipeline run (defaults to a timestamp - YYMMDDHHMMSS) (DUMMY)    │
│    --samples_file       PATH                                 Path YAML file with samples - eg. [{id: ID, files: [F1, F2]}, ...] (samples.yaml)    │
│ *  --bingo_bango        TEXT                                 Some string (REQUIRED)                                                               │
│    --bongo              [foo|bar]                            A string with a limited set of allowed values (bar)                                  │
│    --foo                ARRAY                                A list of integers ([13, 37])                                                        │
│    --bar                MAPPING                              A mapping                                                                            │
│    --baz/--no_baz                                            A boolean (baz)                                                                      │
│    --max_file_size      SIZE                                 The maximum file size in bytes (8)                                                   │
│    --help                                                    Show this message and exit.                                                          │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯

```

Samples can be specified using the `--samples_file` parameter. It takes a single argument, which should be a YAML file describing the samples.

```yaml
# samples.yaml
- id: sample1
  files:
    - /path/to/file1
    - /path/to/file2

- id: sample2
  files:
    - /path/to/file3
    - /path/to/file4
```

> **Note:** All fields specified in the samples file must map to attributes defined in the `Sample` class. The `meta` attribute can be used to add additional metadata to the samples, but using a mixin to add the metadata to the `Sample` class is generally recommended.
