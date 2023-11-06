<p align="center">
  <img src="cellophane.png" width="400px" />
</p>

# Cellophane

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Cellophane is a library for creating modular wrappers.

# ❗️ HERE BE DRAGONS 🐉 ❗️

Cellophane is not battle tested and may break, blow up, and/or eat your pet(s), etc.

## Usage

Cellophane is currently not available on PyPI, but can be installed from git. 

```shell
# You should probably do this in a venv
pip install git+https://github.com/ClinicalGenomicsGBG/cellophane.git
```

A generic project structure can be generated with `cellophane init`.

```shell
# Initialize an empty git repo
git init my_awesome_wrapper
cd my_awesome_wrapper

# Initialize an empty cellophane project
python -m cellophane init my_awesome_wrapper
```

A wrapper directory structure should look something like this:

```
./my_awesome_wrapper/
├── __init__.py
│   # Cellophane module subtree
├── cellophane
│   └── ...
│
│   # Directory containing cellophane modules
├── modules
│   ├── __init__.py
│   ├── my_module.py
│   └── another_module
│       |   # Modules can contain module specific schemas that will be merged with the wrapper schema
│       ├── schema.yaml
│       ├── scripts
│       │   │   # Modules may contain module specific scripts/data
│       │   └── module_script.sh
│       ├── data
│       │   └── some_data.txt
│       └── __init__.py
│
│   # Directory containing scripts to be submitted by Popen, SGE, etc.
├── scripts
│   └── my_script.sh
│
│   # Directory containing misc. files used by the wrapper.
├── scripts
│   └── some_more_data.txt
│
│   # JSON Schema defining configuration options
├── schema.yaml
│
│   # Main entrypoint for the wrapper
└── __main__.py
│
│   # Alternative entrypoint for the wrapper
└── my_awesome_wrapper.py
```

Cellophane includes a CLI to add/remove/update modules

```shell
# Select modules to add from all available modules/versions
python -m cellophane module add

# Add a module at the latest (non-dev) version
python -m cellophane module add my_module@latest

# Update all installed modules. Will individually ask for a branch for each module
python -m cellophane module update

# Update a specific module to a specific version (in this case the dev branch)
python -m cellophane module update my_module@dev

# Select modules to delete from all installed modules
python -m cellophane module remove

# Remove a specific module
python -m cellophane module remove my_module
```

A cellophane wrapper can be run as a script or as a module

```shell
# As a module
python -m my_awesome_wrapper [...]

# As a script
python ./my_awesome_wrapper.py [...]
```

## Configuration

Configuration options must be defined as a JSON Schema in a YAML file. By default, cellophane will use `schema.yaml` located in your project root. The schema will be merged with base schema (`schema.base.yaml` in this repo) which contains configuration options required by all cellophane wrappers.

**NOTES:**

- Cellophane aims to be JSON Schema Draft 7 compliant, but complex schemas may break paring. Please open an issue if you encounter any problems.
- The schema is used to generate the CLI, not to validate the configuration. Rather, the schama will be translated to a `click.Command` which will then parse the configuration.
- These types are implemented in addition to the standard types
  - `path` - Used to specify that a string should be converted to a `pathlib.Path` object.
  - `mapping` - Used to denote `objects` that will be parsed when specified as a string (eg. `foo=bar,baz=qux` will be parsed as `{"foo": "bar", "baz": "qux"}`).
- [Conditional validation](https://json-schema.org/understanding-json-schema/reference/conditionals.html) is supported but has a few caveats:
  - `allOf` will simply combine the schemas without validating against current parameters.
  - `oneOf` will select the *FIRST* schema that validates against current parameters.
  - `anyOf` cannot be used to mark parameters as required, since validation will fail if the parameter is not present.
  - It is possible to "hide" options using conditionals that define properties absent from the base schema (e.g. only expose flag `--a` if flag `--b` is set to `foo`). **This should be used with caution as it will be confusing to users.**

CLI flags for all configuration options specified in the schema are generated automagically. A YAML config file can be passed with the `--config_file` / `-c` flag. Parameters from the config file will be used as defaults for CLI flags. CLI flags will override config file parameters. If a parameter is not specified in the config file or as a CLI flag, the default value specified in the schema will be used.

Required parameters will be evaluated at runtime, using values from the config file or CLI flags. The `--help` flag can be used to list flags and to see what options are still required given the current configuration and flags.

The configuration will be passed to the runners and hooks, via the config keyword argument, as a `Config` object. This object allows for normal dict-like access with string keys (e.g. `config["foo"]["bar"]`) or tuple keyes (e.g. `config["foo", "bar"]`). It also allows for attribute access (e.g. `config.foo.bar`).

**NOTES:**
- Attribute access to `__dunder__` keys works, but is considered as unsupported behaviour.
- The `Config` object implements the `Mapping` protocol, so stuff like `**unpacking` works at any level. *HOWEVER* attribute access to does not work for keys that overlab with a `dict` method (e.g. `keys`, `items`, `values`, etc.). In these cases, use `config["keys"]` instead of `config.keys`.
- The `cellophane.data.to_dict` function can be used to convert a `Config` object to a nested dict.


### Example schema
```yaml
type: object
required:
  - foo
  - bar

properties:
  foo:
    # Nested parameters (type: object) are allowed.
    type: object
    properties:
      baz:
        # Description will be used to generate help messages
        description: Some other parameter
        # Enum is used to limit choices for a parameter 
        enum:
          - HELLO
          - WORLD
        type: string
      # Flags are joined by underscores. (eg. foo.skip will become --foo_skip).
      skip:
        default: false
        description: Skip foo
        # Bools will result in on/off style flags
        type: boolean
    # required needs to be defined on the correct nesting level
    if: {properties: {skip: {const: false}}}
    then: {required: [baz]}
```

This is the resulting auto-generated CLI:

```
$ python -m my_awesome_wrapper


 Usage: my_awesome_wrapper [OPTIONS]

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --workdir                               PATH                                 Output directory                                                                                  │
│ --logdir                                PATH                                 Log directory                                                                                     │
│ --log_level                             [DEBUG|INFO|WARNING|ERROR|CRITICAL]  Log level [INFO]                                                                                  │
│ --samples_file                          PATH                                 Path YAML file with sample names and paths to fastq files (eg. sample: {files: [fastq1, fastq2]}) │
│ --foo_skip/--foo_no_skip                                                     Skip foo [foo_no_skip]                                                                            │
│ --foo_baz                               [HELLO|WORLD]                        Some other parameter                                                                              │
│ --config                                PATH                                 Path to config file                                                                               │
│ --help                                                                       Show this message and exit.                                                                       │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

![Magic](https://i.imgur.com/6KBoYgJ.gif)

## Defining pipeline modules

Runners are functions decrated with `cellophane.modules.runner`, and are responsible for launching the pipeline with the provided samples. They are executed as separate processes in parallel. Optinally, if `individual_samples=True` is specified in the `runner` decorator cellophane will spawn one runner process per sample. A `runner` function will be called with `samples`, `config`, `timestamp`, `label`, `logger`, `root` and `workdir` as keyword arguments.

A module may also define pre/post-hooks. These should be decorated with `cellophane.modules.pre_hook` or `cellophane.modules.post_hook`. Hooks will be executed before or after the whle pipeline completes. Each hook function will be called with `samples`, `config`, `timestamp`, `logger`, and `root` as arguments. 

The main use-case for pre-hooks is to modify `samples` before it is passed to the runners. This can be used to eg. download `.fastq.gz` files from a backup location, decompress `.fasterq` files, add related samples, remove samples with missing files, and so on. If a pre-hook returns a `cellophane.data.Samples` (or a subclass) object it will replace the current `samples`.

The `cellophane.modules.pre_hook` decorator takes optional `before` and `after` arguments. These are lists of hook names that the decorated hook should be executed before or after. An exception will be raised if a circular dependency is detected. It is also possible to set `before` or `after` to `"all"` to execute the hook before or after all other hooks. If multiple hooks have `before` or `after` set to `"all"` the order is random.

The use-case for post-hooks is mayble less obvious, ut they can be used to eg. clean up temporary files, or send an email when a pipeline completes/fails. If your runner returns a `cellophane.data.Samples` (or a subclass) object this will be used by post-hooks, otherwise the original samples will be used. The samples supplied to a post-hook differs from pre-hooks/runners:

1. Samples are combined from all runners. If yout workflow has multiple runners you will get multiple instances of the same sample.
3. Samples will contain `complete` (bool) and `runner` (str) attributes to designate what runner produced them and if it completed

Post-hooks cannot modify the samples object, and so the order in which they are executed is not defined.

**Note:** *Hooks are not module specific, but rather ALL pre-hooks will be executed before ALL runners and ALL post-hooks will be executed afterwards. Module specific functionality should be handeled inside the runner function.*

### Example module

```python
# modules/my_module.py

from pathlib import Path

from cellophane import sge, modules

@modules.pre_hook(priority=10)
def filter_missing(samples, config, timestamp, logger, root):
    _samples = [s for s in samples if all(Path(p).exists() for p in s.files)]
    return Samples(_samples)


@modules.runner()
def foo(samples, config, timestamp, label, logger, root, workdir):

    # config is a UserDict that allows attrubute access
    if not config.foo.skip:
        # Logging is preconfigured
        logger.info("Important information about foo")
        logger.debug(f"Some less important debug information about {config.foo.baz}")

        # Sample sheets for nf-core can be generated automatically
        sample_sheet = samples.nfcore_samplesheet(
            location=worktdir,
            # kwargs will be added as a column to the samplesheet
            # kwargs can also be a sample_id -> value mapping (i.e. a dict)
            strandedness=config.rnafusion.strandedness,
        )


        # sge.submit submits scripts jobs to an SGE cluster (via DRMAA)
        sge.submit(
            str(root / "scripts" / "nextflow.sh"),
            # *args will be passed as arguments to the script
            *(
                f"-log {workdir / 'logs' / 'foo.log'}",
                (
                    # Note that config still behaves as a dict
                    f"-config {config.nextflow.config}"
                    if "config" in config.nextflow
                    else ""
                ),
                f"run {config.nextflow.main}",
                "-ansi-log false",
            ),
            # Environment variables can be passed to the worker node
            env={"MY_VAR": "foo"},
            queue=config.nextflow.sge_queue,
            pe=config.nextflow.sge_pe,
            slots=config.nextflow.sge_slots,
            # Setting check True blocks until the job is done or fails
            check=True,
            name="foo",
            stderr=config.logdir / f"foo.err",
            stdout=config.logdir / f"foo.out",
            cwd=workdir,
        )

# individual_samples=True will spawn one process for each sample
# label="My Fancy Label" lets you use a label other than the function name (primarily for logging)
@modules.runner(
    individual_samples=True,
    label="My Fancy Label"
)
def bar(samples, config, timestamp, logger, root, workdir):

    # samples is still a sequence but length is 1
    sample = samples[0]
    logger.debug(f"Processing {sample.id}")

```

## Mixins

When writing a module, it is sometimes desirable to add functionality to the `cellophane.data.Sample`/`cellophane.data.Samples` classes. This can be achieved by subclassing `cellophane.data.Sample`/`cellophane.data.Sample` in a module. Cellophane will detect these mixins on runtime and, any methods and/or class variables will be added to the relevant class. This can be used to eg. add a `nfcore_samplesheet` method to the `Samples` class. Under the hood, the classes use the `attrs` library, so this most of the functionality of `attrs` is available. However, mixins should not define __init__ methods, as this will break the `attrs` machinery.

**NOTE:** Mixin attributes *MUST* specify a default value. Usually it is a good idea to use `None` as a sentinel value to indicate that the attribute is not set.

### Example mixin

```python
# modules/my_module.py

from cellophane import data

class MySampleMixin:
    some_attr: str = "foo"

class MySamplesMixin(data.Mixin):
    def nfcore_samplesheet(self, location, **kwargs):
        ...
```

## To-do:

- Ensure common exceptions are handled
- Implement missing JSON Schema features (e.g. pattern, format, etc.) if possible
- Add functionality for generating `hydra-genetics` units.tsv/samples.tsv