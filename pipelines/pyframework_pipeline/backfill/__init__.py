"""Step 6: data backfill — transform acquisition outputs into four-layer model.

Reads acquisition manifests and collected artifacts (timing JSON, perf CSV,
asm files) from arm and x86 run directories, then populates Dataset, Source,
and Project layers. Framework is never modified by backfill.
"""
