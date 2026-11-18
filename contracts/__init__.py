"""Data contracts shared by the simulator (producer side) and the Flink jobs (consumer side).

Pure Python plus fastavro, so the rules run in unit tests without Docker. Only `udf.py`
imports PyFlink.
"""
