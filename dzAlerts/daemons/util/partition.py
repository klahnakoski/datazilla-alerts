from dzAlerts.daemons.util.welchs_ttest import welchs_ttest
from dzAlerts.util.env.logs import Log
from dzAlerts.util.queries import Q
from dzAlerts.util.struct import Struct


def partition(series, score_threshold):
    """
    THIS IS TOTALLY FAKE UNTIL I FIND SOFTWARE THAT DOES THIS ALREADY
    (http://arxiv.org/pdf/1309.3295.pdf), OR I DO THE REAL MATH MYSELF.
    """
    output = []
    _partition(series, score_threshold, output)

    # REVIEW THE KNOTS TO ENSURE WE HAVE OPTIMAL PARTITIONS
    for s1, s2 in Q.pairwise(output):
        if welchs_ttest(s1, s2).score < score_threshold:
            Log.error("We seem to have determined a false knot")
    return output


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
