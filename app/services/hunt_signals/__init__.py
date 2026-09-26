"""Hunt signals, one module per family (#3385 spec "Signals").

A signal is ``signal(t, view, constants) -> {series_id: score}``. Its module may import
only the allowlist in ``tests/test_hunt_signal_imports.py`` (no DB driver, reader, file
or network I/O, no randomness), and ``hunt_harness.signal_code_sha256`` hashes the
module file into every trial that uses it. Empty until #3387 opens hunt 1.
"""
