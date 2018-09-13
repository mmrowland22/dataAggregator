import pandas as pd
from collections import OrderedDict


# combines multiple csv files
def combine_data(dflist):
    combodf = pd.concat(dflist, ignore_index=True)
    return combodf


def unwanted_fields(df):
    del df['responseCode']
    del df['responseMessage']
    del df['threadName']
    del df['dataType']
    del df['success']
    del df['failureMessage']
    del df['grpThreads']
    del df['allThreads']
    del df['Latency']
    del df['IdleTime']
    del df['Connect']


def filter_agg_stats(data, pref_list, excludedItems):
    counter = 0
    temp_list = pref_list.copy()
    temp_list.append('label')
    filtered = pd.DataFrame()
    for item in data.label:
        if item not in excludedItems and '/' not in item:
            filtered = filtered.append(data.loc[counter, temp_list])
        counter += 1
    filtered = filtered.sort_values(by=['label'])
    filtered = filtered.reset_index(drop=True)
    return filtered


def num_samples(df):
    df['samples'] = df.groupby(['label'])['elapsed'].transform('count')


def average(df):
    df['average'] = round(df.groupby(['label'])['elapsed'].transform('mean'), 0)


def median(df):
    df['median'] = round(df.groupby(['label'])['elapsed'].transform('median'), 0)


def percentile_90(df):
    df['90th'] = df.groupby(['label'])['elapsed'].transform('quantile', 0.9, interpolation='lower')


def percentile_95(df):
    df['95th'] = df.groupby(['label'])['elapsed'].transform('quantile', 0.95, interpolation='nearest')


def percentile_99(df):
    df['99th'] = df.groupby(['label'])['elapsed'].transform('quantile', 0.99, interpolation='higher')


def min(df):
    df['min'] = df.groupby(['label'])['elapsed'].transform('min')


def max(df):
    df['max'] = df.groupby(['label'])['elapsed'].transform('max')


def sent_kb(df):
    max_ts = (df.groupby(['label'])['timeStamp'].max()).tolist()
    min_ts = (df.groupby(['label'])['timeStamp'].min()).tolist()
    diff = [j - i for i, j in zip(min_ts, max_ts)]
    diff = [i / 1000 for i in diff]
    sent_bytes = (df.groupby(['label'])['sentBytes'].transform('sum')).tolist()
    sent_bytes = list(OrderedDict.fromkeys(sent_bytes))
    sent_bytes = [i / 1024 for i in sent_bytes]
    quo = [j / i for i, j in zip(diff, sent_bytes)]
    quo = [round(num, 2) for num in quo]
    return pd.Series(quo)


def received_kb(df):
    max_ts = (df.groupby(['label'])['timeStamp'].max()).tolist()
    min_ts = (df.groupby(['label'])['timeStamp'].min()).tolist()
    diff = [j - i for i, j in zip(min_ts, max_ts)]
    diff = [i / 1000 for i in diff]
    bytes = (df.groupby(['label'])['bytes'].transform('sum'))
    bytes = list(OrderedDict.fromkeys(bytes))
    bytes = [i / 1024 for i in bytes]
    quo = [j / i for i, j in zip(diff, bytes)]
    quo = [round(num, 2) for num in quo]
    return pd.Series(quo)


def throughput(df):
    requests = df.groupby(['label'])['elapsed'].transform('count')
    labels = df.label.tolist()
    dictionary = dict(zip(labels, requests))
    uniq_labels = []
    uniq_requests = []
    for elem in dictionary:
        if elem not in uniq_labels:
            uniq_labels.append(elem)
            uniq_requests.append(dictionary[elem])
    max_ts = (df.groupby(['label'])['timeStamp'].max()).tolist()
    min_ts = (df.groupby(['label'])['timeStamp'].min()).tolist()
    diff_in_ms = [j - i for i, j in zip(min_ts, max_ts)]
    diff_in_sec = [i / 1000 for i in diff_in_ms]
    quo = [i / j for i, j in zip(uniq_requests, diff_in_sec)]
    quo = [round(num, 5) for num in quo]
    return pd.Series(quo)


def generate_stats(fdf, path, pref_list):
    for pref in pref_list:
        if pref == 'samples':
            num_samples(fdf)
        if pref == 'average':
            average(fdf)
        if pref == 'median':
            median(fdf)
        if pref == '90th':
            percentile_90(fdf)
        if pref == '95th':
            percentile_95(fdf)
        if pref == '99th':
            percentile_99(fdf)
        if pref == 'min':
            min(fdf)
        if pref == 'max':
            max(fdf)
        if pref == 'throughput':
            throughput_calc = throughput(fdf)
        if pref == 'received kb':
            received_calc = received_kb(fdf)
        if pref == 'sent kb':
            sent_calc = sent_kb(fdf)
    del fdf['elapsed']
    del fdf['timeStamp']
    del fdf['sentBytes']
    del fdf['bytes']
    fdf = fdf.drop_duplicates(subset='label')
    fdf = fdf.reset_index(drop=True)
    if pref == 'throughput':
        fdf['throughput'] = throughput_calc
    if pref == 'received kb/sec':
        fdf['received kb/sec'] = received_calc
    if pref == 'sent kb/sec':
        fdf['sent kb/sec'] = sent_calc
    return fdf
