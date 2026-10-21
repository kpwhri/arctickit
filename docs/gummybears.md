---
title: Syntactic Sugar polars Expressions
---

# Syntactic Sugar for `polars` Queries

```python
import polars as pl
from arctickit import TextColumn

code = TextColumn('code')
desc = TextColumn('desc')

result = df.filter(
    (code.starts_with_any(['A12', 'B45']) | code.eq('X01'))
    & ~code.ends_with('99')
    & ~desc.contains('obsolete')
)
```

You may use the short forms in the same expression, such as `code.swany(...)`, `code.eq(...)`, and `code.ew(...)`.

## API reference

| Descriptive method        | Short alias     | Matches                                          |
|---------------------------|-----------------|--------------------------------------------------|
| `starts_with(value)`      | `sw(value)`     | Literal prefix, case-sensitive                   |
| `starts_with_any(values)` | `swany(values)` | Any literal prefix (via `.rdw`)                  |
| `ends_with(value)`        | `ew(value)`     | Literal suffix, case-sensitive                   |
| `ends_with_any(values)`   | `ewany(values)` | Any literal suffix (via `.rdw`)                  |
| `equals(value)`           | `eq(value)`     | Exact equality (`==`)                            |
| `not_equals(value)`       | `ne(value)`     | Exact inequality (`!=`)                          |
| `is_in(values)`           | `eqany(values)` | Exact membership in a sequence                   |
| `not_in(values)`          | —               | Negated exact membership                         |
| `contains(value)`         | —               | Literal substring (regex metacharacters ignored) |
| `contains_any(values)`    | —               | Any literal substring (Polars Aho–Corasick)      |
| `search(pattern)`         | —               | Regex anywhere in the text                       |
| `isearch(pattern)`        | —               | Case-insensitive regex anywhere                  |
| `fullmatch(pattern)`      | —               | Regex must match the **entire** string           |

`column.expr` returns the underlying `pl.col(column.name)` expression. Each predicate returns a `pl.Expr`, rather than a
Python Boolean.

### Literal versus regex matching

```python
code.contains('a.b')  # Literal dot; matches 'a.b', not 'axb'
code.search(r'a.b')  # Regex dot; matches both 'a.b' and 'axb'
code.isearch(r'^ab\d{2}')  # Case-insensitive regex search
code.fullmatch(r'[A-Z]{2}\d{2}')  # Entire value, e.g. 'AB12'
```

Regex methods use the regex syntax accepted by Polars (the Rust `regex` engine), which differs from Python `re` and does
not implement lookaround or backreferences. Full matches use absolute start/end anchors; `(?m)` inside a pattern cannot
make a single line count as a complete match.

### Null and empty input

- Most predicates preserve null values (`None` becomes null), including equality, membership, and negated membership.
  Polars filters retain rows only where the predicate is `True`.
- `starts_with_any([])` and `ends_with_any([])` produce `False` for non-null strings, and null for null strings.
  Including `''` matches every non-null string as a prefix or suffix.
- Single-value prefix and suffix comparisons also match the empty string on non-null text.
- The `values` parameters accept a `Sequence[str]` such as a list or tuple; a plain `str` should be passed only to
  singular methods.
- All operations are case-sensitive except `isearch`.

To include nulls in a negated filter explicitly, use Polars' native `is_null()`:

```python
code = TextColumn('code')
result_df = df.filter(
    code.not_in(['A', 'B']) 
    | code.expr.is_null()
)
```