import numpy
from scipy import stats
import scipy
from dzAlerts import util


def median_test(samples1, samples2, interpolate=True):
    """
    interpolate=True WILL USE FINER VERSION OF THIS TEST
    """
    if len(samples1) < 3 or len(samples2) < 3:
        return {"diff": 0, "confidence": 0}
    median = util.stats.median(samples1 + samples2, simple=not interpolate)

    above1, below1 = count_partition(samples1, median)
    above2, below2 = count_partition(samples2, median)

    result = scipy.stats.chisquare(
        numpy.array([above1, below1, above2, below2]),
        f_exp=numpy.array([float(len(samples1)) / 2, float(len(samples1)) / 2, float(len(samples2)) / 2, float(len(samples2)) / 2])
    )
    return {"diff": result[0], "confidence": 1-result[1]}


def count_partition(samples, cut_value, resolution=1.0):
    """
    COMPARE SAMPLES TO cut_value AND COUNT IF GREATER OR LESSER
    """
    smaller = 0.0
    larger = 0.0
    min_cut = cut_value - resolution/2.0
    max_cut = cut_value + resolution/2.0
    for v in samples:
        if v > max_cut:
            larger += 1
        elif v < min_cut:
            smaller += 1
        else:
            larger += (v - min_cut) / resolution
            smaller += (max_cut - v) / resolution
    return smaller, larger

