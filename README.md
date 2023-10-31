<div style="display: flex; align-items: center; flex-direction: column;">
<img src="cellophane.png" height="600px" />
</div>

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
python -m cellophane init
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

Pre-made modules can also be added from git

```shell
# Add the module repo as a remote and install a module as a subtree
git remote add -f modules https://github.com/ClinicalGenomicsGBG/cellophane_modules
git subtree add --prefix modules/slims modules slims --squash -m "Add SLIMS module"

# Upgrading is done the same way as cellophane
git subtree pull --prefix modules/slims modules slims --squash -m "Upgrade SLIMS module"
```

A cellophane wrapper can be run as a script or as a module

```shell
# As a module
python -m my_awesome_wrapper [...]

# As a script
python ./my_awesome_wrapper.py [...]
```

## Configuration

Configuration options must be defined as a JSON Schema in a YAML file. By default, cellophane will use `schema.yaml` located in your project root. The schema will be merged with base schema (`schema.base.yaml` in this repo) which contains configuration options required by all cellophane wrappers (eg. SLIMS API user/key). **NOTE:** *Most features should be supported, but complex schemas may break the custom JSONSchema parisng done by cellophane*

CLI flags for all configuration options specified in the schema are generated automagically. A YAML config file can be passed with the `--config_file` / `-c` flag.

### Example schema
```yaml
type: object
required:
  - foo
  - bar

properties:
  foo:
    # Nested parameters (type: object) are allowed.
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
    required:
      - baz
    type: object
```

This is the resulting auto-generated CLI:

```shell
$ python -m my_awesome_wrapper


 Usage: my_awesome_wrapper [OPTIONS]

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --outdir                                PATH                                 Output directory                                                                                  │
│ --logdir                                PATH                                 Log directory                                                                                     │
│ --log_level                             [DEBUG|INFO|WARNING|ERROR|CRITICAL]  Log level [INFO]                                                                                  │
│ --samples_file                          PATH                                 Path YAML file with sample names and paths to fastq files (eg. sample: {files: [fastq1, fastq2]}) │
│ --foo_skip                                                                   Skip foo                                                                                          │
│ --foo_baz                               [HELLO|WORLD]                        Some other parameter                                                                              │
│ --config                                PATH                                 Path to config file                                                                               │
│ --help                                                                       Show this message and exit.                                                                       │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

![Magic](https://i.imgur.com/6KBoYgJ.gif)

## Defining pipeline modules

At least one module must contain at least one `cellophane.modules.runner` decorated function. Runners are responsible for launching the pipeline with the provided samples. They are executed as separate processes in parallel. Optinally, if `individual_samples=True` is specified in the `runner` decorator cellophane will spawn one runner process per sample. A `runner` function will be called with `samples`, `config`, `timestamp`, `label`, `logger`, `root` and `outdir` as keyword arguments.

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
def foo(samples, config, timestamp, label, logger, root, outdir):

    # config is a UserDict that allows attrubute access
    if not config.foo.skip:
        # Logging is preconfigured
        logger.info("Important information about foo")
        logger.debug(f"Some less important debug information about {config.foo.baz}")

        # Sample sheets for nf-core can be generated automatically
        sample_sheet = samples.nfcore_samplesheet(
            location=outdir,
            # kwargs will be added as a column to the samplesheet
            # kwargs can also be a sample_id -> value mapping (i.e. a dict)
            strandedness=config.rnafusion.strandedness,
        )


        # sge.submit submits scripts jobs to an SGE cluster (via DRMAA)
        sge.submit(
            str(root / "scripts" / "nextflow.sh"),
            # *args will be passed as arguments to the script
            *(
                f"-log {outdir / 'logs' / 'foo.log'}",
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
            cwd=outdir,
        )

# individual_samples=True will spawn one process for each sample
# label="My Fancy Label" lets you use a label other than the function name (primarily for logging)
@modules.runner(
    individual_samples=True,
    label="My Fancy Label"
)
def bar(samples, config, timestamp, logger, root, outdir):

    # samples is still a sequence but length is 1
    sample = samples[0]
    logger.debug(f"Processing {sample.id}")

```

## Mixins

When writing a module, it is sometimes desirable to add functionality to the `cellophane.data.Sample`/`cellophane.data.Samples` classes. This can be achieved by subclassing `cellophane.data.Sample`/`cellophane.data.Sample` in a module. Cellophane will detect these mixins on runtime and, any methods and/or class variables will be added to the relevant class. This can be used to eg. add a `nfcore_samplesheet` method to the `Samples` class. Under the hood, the classes use the `attrs` library, so this most of the functionality of `attrs` is available. However, mixins should not define __init__ methods, as this will break the `attrs` machinery.

**Note:** *Mixin attributes MUST specify a default value. Usually it is a good idea to use `None` as a sentinel value to indicate that the attribute is not set.*

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

- Improve logging to file
- Make exception handling less convoluted
- Add functionality for generating `hydra-genetics` units.tsv/samples.tsv
- Add functionality to cellophane for fetching modules from github