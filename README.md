# Cellophane

A library for creating modular wrappers.

## Usage

Add cellophane as a subtree at the base of your project. A generic project structure can be generated with `cellophane init`.

```shell
# Initialize an empty git repo
git init my_awesome_wrapper
cd my_awesome_wrapper

# Add the cellophane repo as a remote and install cellophane as a subtree in the project root
git remote add -f cellophane https://github.com/dodslaser/cellophane
git subtree add --prefix cellophane cellophane main --squash

# Initialize an empty cellophane project
python -m cellophane init --path .

# To upgrade to the latest commit
git fetch cellophane
git subtree pull --prefix cellophane cellophane main --squash -m "Upgrade cellophane"
```

Pre-made modules can also be added from git

```shell
# Add the module repo as a remote and install a module as a subtree
git remote add -f modules https://github.com/dodslaser/cellophane_modules
git subtree add --prefix modules/hcp modules hcp --squash -m "Add HCP module"

# Upgrading is done the same way as cellophane
git fetch modules
git subtree pull --prefix modules/hcp modules hcp --squash -m "Upgrade HCP module"
```



A wrapper directory structure should look something like this:

```
./my_awesome_wrapper/
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

If for some reason you need to change this structure you can change `__main__.py` as such:

```python
from cellophane import cellophane

if __name__ == "__main__":
    main = cellphane(
        "My Awesome Wrapper" # Will be used for logging
        scripts_path=Path(__file__) / "scripts" # Not required if scripts are in the default location (./scripts)
        modules_path=Path(__file__) / "scripts" # Not required if modules are in the default location (./modules)
        schema_path=Path(__file__) / "schema.yaml" # Not required if modules are in the default location (./schema.yaml)
    )

    main(prog_name="my_awesome_wrapper")
```

A cellophane wrapper can be run as a script or as a module

```shell
# As a module
python -m my_awesome_wrapper [...]

# As a script
python ./my_awesome_wrapper.py [...]
```

## Configuration

Configuration options must be defined as a JSON Schema in a YAML file. By default, cellophane will use `schema.yaml` located in your project root. To oveerride use `cellophane.cellophane(schema_path=Path(...))`. The schema will be merged with base schema (`schema.yaml` in this repo) which contains configuration options required by all cellophane wrappers (eg. SLIMS API user/key). **NOTE:** *More complex JSON schemas may work, but are also more likely break the parsing.*

CLI flags for all configuration options specified in the schema are generated automagically. A YAML config file can be passed with the `--config_file` / `-c` flag.

Here's the bease `schema.yaml` as an example:

```yaml
type: object
required:
  - foo
  - bar
  - nextflow

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

  nextflow:
    properties:
      cluster_options:
        description: Nextflow cluster options
        type: string
      config:
        description: Nextflow config file
        # The path type can be used to desingate strings that should be treated as paths 
        type: path
      profile:
        description: Nextflow profile
        type: string
      resume:
        default: false
        description: Resume previous nextflow run
        type: boolean
      sge_pe:
        description: SGE parallel environment for nextflow manager
        type: string
      sge_queue:
        description: SGE queue for nextflow manager
        type: string
      sge_slots:
        description: SGE slots (threads) for nextflow manager
        type: integer
      workdir:
        description: Nextflow work directory
        type: path
    required:
      - sge_queue
      - sge_pe
      - sge_slots
      - profile
      - workdir
    type: object
```

This is the resulting auto-generated CLI:

```shell
$ python -m my_awesome_wrapperrunpy


 Usage: my_awesome_wrapper [OPTIONS]

╭─ Options ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --outdir                                PATH                                 Output directory                                                                        │
│ --logdir                                PATH                                 Log directory                                                                           │
│ --log_level                             [DEBUG|INFO|WARNING|ERROR|CRITICAL]  Log level [default: INFO]                                                               │
│ --samples_file                          PATH                                 Path YAML file with sample names and paths to fastq files (eg. sample: [fastq1, fastq2] │
│ --foo_skip                                                                   Skip foo                                                                                │
│ --foo_baz                               [HELLO|WORLD]                        Some other parameter                                                                    │
│ --nextflow_cluster_options              TEXT                                 Nextflow cluster options                                                                │
│ --nextflow_workdir                      PATH                                 Nextflow work directory                                                                 │
│ --nextflow_resume                                                            Resume previous nextflow run                                                            │
│ --nextflow_profile                      TEXT                                 Nextflow profile                                                                        │
│ --nextflow_config                       PATH                                 Nextflow config file                                                                    │
│ --nextflow_sge_slots                    INTEGER                              SGE slots (threads) for nextflow manager                                                │
│ --nextflow_sge_pe                       TEXT                                 SGE parallel environment for nextflow manager                                           │
│ --nextflow_sge_queue                    TEXT                                 SGE queue for nextflow manager                                                          │
│ --config                                PATH                                 Path to config file                                                                     │
│ --help                                                                       Show this message and exit.                                                             │
╰──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

![Magic](https://i.imgur.com/6KBoYgJ.gif)

## Defining pipeline modules

At least one module must contain at least one `runner` decorated function. Runners are responsible for launching the pipeline with the provided samples. They are executed as separate processes in parallel. Optinally, if `individual_samples=True` is specified in the `runner` decorator cellophane will spawn one runner process per sample. A `runner` function must accept `label`, `samples`, `config`, and `logger` as arguments.

A module may also define pre/post-hooks. These are functions that will be executed before or after the whle pipeline completes. A hook function must take `config`, `samples`, amd `logger` as arguments. Hooks are executed sequentially and can be given a numeric priority to ensure correct execution order. By default the priority will be `inf`. Setting a lower `priority` means the hook will be executed earlier.

The main use-case for pre-hooks is to modify `samples` before it is passed to the runners. This can be used to eg. download `.fastq.gz` files from a backup location, decompress `.fasterq` files, add related samples, remove samples with missing files, and so on. If a pre-hook returns a `cellophane.data.Samples` (or a subclass) object it will replace the current `samples`. The use-case for post-hooks is mayble less obvious, ut they can be used to eg. clean up temporary files, or send an email when a pipeline completes/fails.

Note that hooks are not module specific, but rather ALL pre-hooks will be executed before ALL runners and ALL post-hooks will be executed afterwards. Module specific functionality should be handeled inside the runner function.

```python
# modules/my_module.py

from pathlib import Path

from cellophane import sge, modules

@modules.pre_hook(priority=10)
def filter_missing(config, samples, logger, scripts_path):
    _samples = [s for s in samples if all(Path(p).exists() for p in s.fastq_paths)]
    return Samples(_samples)


@modules.runner()
def foo(label, samples, config, logger, scripts_path):

    # config is a UserDict that allows attrubute access
    if not config.foo.skip:
        # Logging is preconfigured
        logger.info("Important information about foo")
        logger.debug(f"Some less important debug information about {config.foo.baz}")

        # Note that in reality outdir must be accessible on all SGE nodes
        outdir = Path("/tmp/foo")

        # Sample sheets for nf-core can be generated automatically
        sample_sheet = samples.nfcore_samplesheet(
            location=outdir,
            # kwargs will be added as a column to the samplesheet
            # kwargs can also be a sample_id -> value mapping (i.e. a dict)
            strandedness=config.rnafusion.strandedness,
        )


        # sge.submit submits scripts jobs to an SGE cluster (via DRMAA)
        sge.submit(
            str(scripts_path / "nextflow.sh"),
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
def bar(label, samples, config, logger):

    # samples is still a sequence but length is 1
    sample = samples[0]
    logger.debug(f"Processing {sample.id}")

```

## To-do:

- Improve logging to file
- Handle of complex JSON schemas
- Add functionality for generating `hydra-genetics` units.tsv/samples.tsv
- Make hook priority overridable
