"""Per-domain collectors.

Every collector exposes ``collect(client, config, **kwargs) -> dict``
returning the uniform ``{status, reason, summary, detail}`` shape
defined in CLAUDE.md.
"""
