"""Data acquisition module — merge of original steps 5 (case data) and 6 (perf data).

Three sub-steps:
  5a. Timing data — parse benchmark_runner TM stdout
  5b. Perf profile — process perf.data via python-performance-kits
  5c. Machine code — perf annotate + objdump
"""
