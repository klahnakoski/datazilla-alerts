
def single_ttest(point, stats, min_variance=0):
    n1 = stats.count
    m1 = stats.mean
    v1 = max(stats.variance, 1.0/12.0)  # VARIANCE OF STANDARD UNIFORM DISTRIBUTION

    if n1 < 2:
        return {"confidence": 0, "diff": 0}

    try:
        tt = (point - m1) / sqrt(v1)
        t_distribution = scipy.stats.distributions.t(n1 - 1)
        confidence = t_distribution.cdf(tt)
        return {"confidence": confidence, "diff": tt}
    except Exception, e:
        Log.error("error with t-test", e)
