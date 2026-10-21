"""Readable, composable string predicates for Polars expressions."""

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl
import polars_cnlp  # noqa: F401  # Registers the .rdw expression namespace.


@dataclass(frozen=True)
class TextColumn:
    """Build reusable Boolean expressions for a named Polars string column.

    Every predicate returns a ``pl.Expr``, so it can be combined with ``&``,
    ``|``, and ``~``, or used in eager and lazy queries. Unless noted otherwise,
    comparisons are case-sensitive and return null for null column values.

    Args:
        name: Name of a String column in a Polars DataFrame.
    """

    name: str

    @property
    def expr(self) -> pl.Expr:
        """Return the underlying ``pl.col(name)`` expression."""
        return pl.col(self.name)

    def starts_with(self, value: str) -> pl.Expr:
        """Test whether text starts with a literal, case-sensitive prefix."""
        return self.expr.str.starts_with(value)

    def sw(self, value: str) -> pl.Expr:
        """Short for ``starts_with`` (literal prefix match)."""
        return self.starts_with(value)

    def starts_with_any(self, values: Sequence[str]) -> pl.Expr:
        """Test whether text starts with any literal prefix (``rdw`` extension)."""
        return self.expr.rdw.starts_with_any(list(values))

    def swany(self, values: Sequence[str]) -> pl.Expr:
        """Short for ``starts_with_any`` (match any literal prefix)."""
        return self.starts_with_any(values)

    def ends_with(self, value: str) -> pl.Expr:
        """Test whether text ends with a literal, case-sensitive suffix."""
        return self.expr.str.ends_with(value)

    def ew(self, value: str) -> pl.Expr:
        """Short for ``ends_with`` (literal suffix match)."""
        return self.ends_with(value)

    def ends_with_any(self, values: Sequence[str]) -> pl.Expr:
        """Test whether text ends with any literal suffix (``rdw`` extension)."""
        return self.expr.rdw.ends_with_any(list(values))

    def ewany(self, values: Sequence[str]) -> pl.Expr:
        """Short for ``ends_with_any`` (match any literal suffix)."""
        return self.ends_with_any(values)

    def equals(self, value: str) -> pl.Expr:
        """Test exact, case-sensitive equality (Polars ``==``)."""
        return self.expr == value

    def eq(self, value: str) -> pl.Expr:
        """Short for ``equals`` (exact, case-sensitive equality)."""
        return self.equals(value)

    def not_equals(self, value: str) -> pl.Expr:
        """Test exact, case-sensitive inequality (Polars ``!=``)."""
        return self.expr != value

    def ne(self, value: str) -> pl.Expr:
        """Short for ``not_equals`` (exact, case-sensitive inequality)."""
        return self.not_equals(value)

    def is_in(self, values: Sequence[str]) -> pl.Expr:
        """Test membership in a collection of exact, case-sensitive strings."""
        return self.expr.is_in(list(values))

    def eqany(self, values: Sequence[str]) -> pl.Expr:
        """Short for ``is_in`` (equal to any string in the collection)."""
        return self.is_in(values)

    def not_in(self, values: Sequence[str]) -> pl.Expr:
        """Test non-membership in a collection; null values stay null."""
        return ~self.is_in(values)

    def contains(self, value: str) -> pl.Expr:
        """Test for a literal, case-sensitive substring (not a regex)."""
        return self.expr.str.contains(value, literal=True)

    def contains_any(self, values: Sequence[str]) -> pl.Expr:
        """Test whether any literal, case-sensitive substring occurs in text."""
        return self.expr.str.contains_any(list(values))

    def search(self, pattern: str) -> pl.Expr:
        """Search for a Rust-style regex anywhere in the string (case-sensitive)."""
        return self.expr.str.contains(pattern)

    def isearch(self, pattern: str) -> pl.Expr:
        """Search for a regex anywhere, ignoring case with a scoped ``(?i)`` flag."""
        return self.search(f'(?i:{pattern})')

    def fullmatch(self, pattern: str) -> pl.Expr:
        """Match the entire string with a regex, even when multiline mode is on."""
        return self.search(rf'\A(?:{pattern})\z')
