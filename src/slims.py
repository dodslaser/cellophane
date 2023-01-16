"""Utilities for interacting with SLIMS"""

from collections import UserList
from json import loads
from pathlib import Path
from time import time
from typing import Mapping, Optional
from yaml import safe_load

from humanfriendly import parse_timespan
from slims.criteria import (
    Criterion,
    conjunction,
    disjunction,
    contains,
    equals,
    greater_than_or_equal,
    is_one_of,
)
from slims.slims import Record, Slims

from . import util


class Content:
    """Content types"""

    DNA = 6
    FASTQ = 22
    BIOINFORMATICS = 23


def get_records(
    connection: Slims,
    *args: Criterion,
    slims_id: Optional[str | list[str]] = None,
    max_age: Optional[int | str] = None,
    analysis: Optional[int | list[int]] = None,
    content_type: Optional[int | list[int]] = None,
    **kwargs: str | int | list[str | int],
) -> list[Record]:
    """Get records from SLIMS"""

    criteria = conjunction()

    match slims_id:
        case str():
            criteria = criteria.add(equals("cntn_id", slims_id))
        case [*ids]:
            criteria = criteria.add(is_one_of("cntn_id", ids))
        case _ if slims_id is not None:
            raise TypeError(f"Invalid type for id: {type(slims_id)}")

    match max_age:
        case int() | str():
            min_ctime = int(time() - parse_timespan(str(max_age))) * 1e3
            criteria = criteria.add(greater_than_or_equal("cntn_createdOn", min_ctime))
        case _ if max_age is not None:
            raise TypeError(f"Expected int or str, got {type(max_age)}")

    match analysis:
        case None:
            pass
        case int():
            criteria = criteria.add(
                disjunction()
                .add(contains("cntn_cstm_secondaryAnalysis", analysis))
                .add(equals("cntn_cstm_secondaryAnalysis", analysis))
            )
        case [*_, int()] as analysis:
            _analysis = disjunction()
            for individual_analysis in analysis:
                _analysis = _analysis.add(
                    disjunction()
                    .add(contains("cntn_cstm_secondaryAnalysis", individual_analysis))
                    .add(equals("cntn_cstm_secondaryAnalysis", individual_analysis))
                )
            criteria = criteria.add(_analysis)
        case _:
            raise TypeError(f"Expected int(s), got {type(analysis)}")

    match content_type:
        case None:
            pass
        case int():
            criteria = criteria.add(equals("cntn_fk_contentType", content_type))
        case [*_, int()]:
            criteria = criteria.add(is_one_of("cntn_fk_contentType", content_type))
        case _:
            raise TypeError(f"Expected int(s), got {type(content_type)}")

    for key, value in kwargs.items():
        criteria = criteria.add(
            is_one_of(key, [value] if isinstance(value, int | str) else value)
        )

    for arg in args:
        criteria = criteria.add(arg)

    return connection.fetch("Content", criteria)


def get_derived_records(
    connection: Slims,
    derived_from: Record | list[Record],
    *args,
    **kwargs,
) -> dict[Record, Record | None]:
    """Get derived records from SLIMS"""
    match derived_from:
        case record if isinstance(record, Record):
            original = {record.pk(): record}
        case [*records] if all(isinstance(r, Record) for r in records):
            original = {r.pk(): r for r in records}
        case _:
            raise TypeError(f"Expected Record(s), got {derived_from}")

    criterion = is_one_of("cntn_fk_originalContent", [*original])
    records = get_records(connection, criterion, *args, **kwargs)

    return {o: None for o in original.values()} | {
        original[r.cntn_fk_originalContent.value]: r for r in records  # type: ignore
    }


class Samples(UserList):
    """A list of sample containers"""

    @classmethod
    def novel(cls, connection: Slims, analysis: int, content_type: int, create=False):
        """Get novel samples"""
        if content_type == Content.DNA:
            _dna = get_records(
                connection,
                analysis=analysis,
                content_type=Content.DNA,
            )

            _fastqs = [
                v
                for v in get_derived_records(
                    connection,
                    derived_from=_dna,
                    content_type=Content.FASTQ,
                ).values()
                if v is not None
            ]

        elif content_type == Content.FASTQ:
            _fastqs = get_records(
                connection,
                analysis=analysis,
                content_type=Content.FASTQ,
            )

        else:
            raise ValueError(f"Invalid content type: {content_type}")

        _bioinformatics = get_derived_records(
            connection,
            derived_from=_fastqs,
            content_type=Content.BIOINFORMATICS,
        )

        if create:
            for original in [o for o, d in _bioinformatics.items() if d is None]:
                fields = {
                    "cntn_id": original.cntn_id.value,  # type: ignore
                    "cntn_fk_contentType": Content.BIOINFORMATICS,
                    "cntn_status": 10,  # Pending
                    "cntn_fk_location": 83,  # FIXME: Should location be configuarable?
                    "cntn_fk_originalContent": original.pk(),
                    "cntn_fk_user": "",  # FIXME: Should user be configuarable?
                    "cntn_cstm_SecondaryAnalysisState": "novel",
                    "cntn_cstm_secondaryAnalysisBioinfo": analysis,
                }
                _bioinformatics[original] = connection.add("Content", fields)
        else:
            _bioinformatics = {k: v for k, v in _bioinformatics.items() if v is None}

        return cls.from_records(
            fastqs=_fastqs,
            bioinformatics=[*_bioinformatics.values()],
        )

    @classmethod
    def from_slims(cls, connection: Slims, *args, **kwargs):
        """Get samples from SLIMS"""
        _fastqs = get_records(connection, *args, **kwargs)
        _bioinformatics = get_derived_records(
            connection,
            derived_from=_fastqs,
            content_type=Content.BIOINFORMATICS,
        )
        return cls.from_records(_fastqs, [*_bioinformatics.values()])

    @classmethod
    def from_file(cls, path: Path):
        """Get samples from a YAML file"""
        with open(path, "r", encoding="utf-8") as handle:
            samples = [
                util.Container(
                    pk=None,
                    id=str(_id),
                    fastq_paths=[fastq1, fastq2],
                    bioinformatics=None,
                    backup=None,
                )
                for _id, (fastq1, fastq2) in safe_load(handle).items()
            ]
        return cls(samples)

    @classmethod
    def from_ids(cls, connection: Slims, ids: list[str]) -> "Samples":
        """Get samples from SLIMS by ID"""
        _fastqs = get_records(connection, content_type=Content.FASTQ, slims_id=ids)
        _bioinformatics = get_derived_records(
            connection,
            derived_from=_fastqs,
            content_type=Content.BIOINFORMATICS,
        )
        return cls.from_records(_fastqs, [*_bioinformatics.values()])

    @classmethod
    def from_records(cls, fastqs: list[Record], bioinformatics: list[Record | None]):
        """Get samples from SLIMS records"""
        _fastqs = {f.pk(): f for f in fastqs}
        _bioinformatics = {b.cntn_fk_originalContent.value: b for b in bioinformatics}  # type: ignore
        _demuxer = {
            f.pk(): {**loads(f.cntn_cstm_demuxerSampleResult.value)} for f in fastqs  # type: ignore
        }
        _backup = {
            f.pk(): {**loads(f.cntn_cstm_demuxerBackupSampleResult.value)}  # type: ignore
            for f in fastqs
        }

        return cls(
            [
                util.Container(
                    pk=pk,
                    id=_fastqs[pk].cntn_id.value,  # type: ignore
                    fastq=_fastqs[pk],
                    bioinformatics=_bioinformatics[pk],
                    backup=_backup[pk],
                    **_demuxer[pk],
                )
                for pk in _fastqs
            ]
        )

    def hydra_units_samples(self, *_, location: str = "samples", **kwargs):
        """Write Hydra units and samples files"""
        # Path(location).mkdir(parents=True, exist_ok=True)
        # _units_path = Path(location) / "units.csv"
        # _samples_path = Path(location) / "samples.csv"
        # with (
        #     open(_units_path, "w", encoding="utf-8") as units,
        #     open(_samples_path, "w", encoding="utf-8") as samples,
        # ):
        #     pass

        # FIXME: Implement hydra units and samples files
        raise NotImplementedError

    def nfcore_samplesheet(self, *_, location: str | Path, **kwargs) -> Path:
        """Write a Nextflow samplesheet"""
        Path(location).mkdir(parents=True, exist_ok=True)
        _data = [
            {
                "sample": sample.id,
                "fastq_1": sample.fastq_paths[0],
                "fastq_2": sample.fastq_paths[1],
                **{
                    k: v[sample.id] if isinstance(v, Mapping) else v
                    for k, v in kwargs.items()
                },
            }
            for sample in self
        ]

        _header = ",".join(_data[0].keys())

        _samplesheet = "\n".join([_header, *(",".join(d.values()) for d in _data)])
        _path = Path(location) / "samples.nextflow.csv"
        with open(_path, "w", encoding="utf-8") as handle:
            handle.write(_samplesheet)

        return _path

    def update_bioinformatics(self, state: str) -> None:
        """Update bioinformatics state in SLIMS"""
        match state:
            case "running" | "complete" | "error":
                for sample in self:
                    if sample.bioinformatics is not None:
                        sample.bioinformatics = sample.bioinformatics.update(
                            {"cntn_cstm_SecondaryAnalysisState": state}
                        )
            case _:
                raise ValueError(f"Invalid state: {state}")
