#!/usr/bin/env bash

set -eo pipefail

case "$(uname -sm)" in
  "Linux x86_64") PLATFORM=linux; ARCH=64;;
  "Linux aarch64") PLATFORM=linux; ARCH=aarch64;;
  "Linux ppc64le") PLATFORM=linux; ARCH=ppc64le;;
  "Darwin x86_64") PLATFORM=osx; ARCH=64;;
  "Darwin arm64") PLATFORM=osx; ARCH=arm64;;
  *) echo "Unsupported platform: $(uname -sm)"; exit 1;;
esac

if curl -V; then
  DL="curl -L"
elif wget -V; then
  DL="wget -O-"
else
  echo "Neither curl nor wget found";
  exit 1
fi

mkdir -p "${TMPDIR}/mamba"
$DL "https://micro.mamba.pm/api/micromamba/${PLATFORM}-${ARCH}/latest" | tar -xvjC "${TMPDIR}/mamba" "bin/micromamba"
eval "$("${TMPDIR}/mamba/bin/micromamba" shell hook -s posix)"
micromamba env create -p "${TMPDIR}/mamba/${_CONDA_ENV_NAME}" -f "${_CONDA_ENV_SPEC}"
micromamba run -p "${TMPDIR}/mamba/${_CONDA_ENV_NAME}" "$@"