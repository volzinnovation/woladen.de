from __future__ import annotations

import gzip
import io


def buffered_stream(raw_stream: io.RawIOBase | io.BufferedIOBase) -> io.BufferedIOBase:
    """Return *raw_stream* wrapped in ``io.BufferedReader`` when it lacks ``peek()``.

    Pass the result to :func:`binary_stream_from_binary_stream` or
    :func:`text_stream_from_binary_stream` when the caller cannot guarantee the
    incoming stream is already buffered (e.g. ``io.BytesIO``).
    """
    if hasattr(raw_stream, "peek"):
        return raw_stream  # type: ignore[return-value]
    return io.BufferedReader(raw_stream)  # type: ignore[arg-type]


def binary_stream_from_binary_stream(
    raw_stream: io.RawIOBase | io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> io.BufferedIOBase | gzip.GzipFile:
    """Return a (possibly decompressed) binary stream.

    Transparently handles gzip-compressed input detected via the
    *content_encoding* header **or** the ``\\x1f\\x8b`` magic bytes.
    Auto-wraps the stream in ``io.BufferedReader`` when it lacks ``peek()``.
    """
    stream = buffered_stream(raw_stream)
    start: bytes = stream.peek(2)[:2]  # type: ignore[union-attr]
    if content_encoding.casefold() == "gzip" or start == b"\x1f\x8b":
        return gzip.GzipFile(fileobj=stream)
    return stream


def text_stream_from_binary_stream(
    raw_stream: io.RawIOBase | io.BufferedIOBase,
    *,
    content_encoding: str = "",
    encoding: str = "utf-8",
    newline: str | None = None,
) -> io.TextIOWrapper:
    """Return a text-mode wrapper around a (possibly gzip-compressed) binary stream.

    The *encoding* parameter defaults to ``"utf-8"`` but can be set to e.g.
    ``"utf-8-sig"`` for CSV sources that include a BOM.  Set *newline* to
    ``""`` when wrapping a stream for ``csv.reader`` / ``csv.DictReader`` to
    prevent universal newline translation (per Python CSV documentation).

    Callers are responsible for calling ``.detach()`` on the returned wrapper
    when done to avoid double-closing the underlying binary stream.
    """
    return io.TextIOWrapper(
        binary_stream_from_binary_stream(raw_stream, content_encoding=content_encoding),
        encoding=encoding,
        newline=newline,
    )
