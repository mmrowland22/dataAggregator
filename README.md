# dataAggregator
This script reads a file of aggregated data from a performance test step to generate a statistics report and histogram.

The statistics report can contain: sample size, average, median, 90th percentile, 95th percentile, 99th percentile, throughput, received kb/sec and sent kb/sec

The input is load from a yaml file containing the root directory, the preferred fields for the stats report, the items to exclude and the sorting order.

This is accomplished through Python using the yaml, pandas, numpy, os, re, and matplotlib libraries.
