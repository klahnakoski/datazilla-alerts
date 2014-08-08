from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from dzAlerts.util.struct import Struct


def partition(series, score_threshold):
    output = []
    _partition(series, score_threshold, output)


def _partition(series, score_threshold, output):
    best = Struct(index=-1, score=0)
    for i, s in enumerate(series):
        candidate = Struct(index=i, score=welchs_ttest(series[:i], series[1:]).score)
        if candidate.score > score_threshold and candidate.score > best.score:
            best = candidate
    if best.index == -1:
        output.append(series)
    else:
        _partition(series[:best.index], score_threshold, output)
        _partition(series[best.index:], score_threshold, output)
