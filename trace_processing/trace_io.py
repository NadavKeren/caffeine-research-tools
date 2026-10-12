import io
import lzma
import tarfile
from compression import zstd
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, TextIO


def is_tar(path: Path) -> bool:
    return path.suffix == '.tgz' or '.tar' in path.suffixes


@contextmanager
def open_trace(path: Path, **kwargs) -> Iterator[TextIO]:
    """
        Open a trace file for reading as text.
        Files ending with .zst or .xz are decompressed on the fly, so there is no need to extract them beforehand.
        For tar archives (.tgz, .tar.gz, ...) the first regular file in the archive is read, also on the fly.
    """
    if is_tar(path):
        # The members are read lazily, so reaching the first file does not scan or extract the rest of the archive.
        with tarfile.open(path) as archive:
            for member in archive:
                if member.isfile():
                    with io.TextIOWrapper(archive.extractfile(member), **kwargs) as trace:
                        yield trace
                    return
            raise ValueError(f'No regular file found in the archive {path}')
    elif path.suffix == '.zst':
        with zstd.open(path, 'rt', **kwargs) as trace:
            yield trace
    elif path.suffix == '.xz':
        with lzma.open(path, 'rt', **kwargs) as trace:
            yield trace
    else:
        with path.open('r', **kwargs) as trace:
            yield trace


if __name__ == "__main__":
    print("Not intended to be run")
    exit(1)
