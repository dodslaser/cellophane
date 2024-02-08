<!-- markdownlint-disable MD033 Used for centered logo -->
<!-- markdownlint-disable MD036 -->

# Cellophane

<p align="center">
  <img src="cellophane.png" width="400px" alt="Cellophane Logo"/>
</p>

[![codecov](https://codecov.io/gh/ClinicalGenomicsGBG/cellophane/graph/badge.svg?token=GQ6MOR6CYL)](https://codecov.io/gh/dodslaser/cellophane)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![MegaLinter](https://github.com/ClinicalGenomicsGBG/cellophane/workflows/MegaLinter/badge.svg)](https://github.com/ClinicalGenomicsGBG/cellophane/actions?query=workflow%3AMegaLinter)

---

Cellophane is a library for creating modular wrappers. The purpose is both to facilitate wrapping a pipeline with a common framework, and also to simplify the process of porting the wrapper to a different HPC environment, LIMS, long term storage, etc.

**❗️ HERE BE DRAGONS ❗️**

Cellophane is not battle tested and may break, blow up, and/or eat your pet(s), etc.

## Usage

See [USAGE.md](USAGE.md) for a detailed explanation of how to use cellophane.

## To-do

- Ensure common exceptions are handled
- Implement missing JSON Schema features (e.g. pattern, format, etc.) if possible
- Add functionality for generating `hydra-genetics` units.tsv/samples.tsv
