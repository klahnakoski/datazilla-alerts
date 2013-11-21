################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################
## http://people.mozilla.org/~klahnakoski/chance_is_failure.html FOR EXAMPLES
################################################################################




from dzAlerts.daemons.util.failrate import failure_rate, confident_fail_rate
from dzAlerts.util.logs import Log
from dzAlerts.util import startup


EPSILON = 0.000001


def closeEnough(a, b):
    if abs(a - b) <= EPSILON * (abs(a) + abs(b)):
        return True
    return False


previous_results = {
    "test_0": {
        "total_pass": 90,
        "total_fail": 10
    },
    "test_1": {
        "total_pass": 99,
        "total_fail": 1
    }
}

def test_a_pass_b_fail():
    result = failure_rate(previous_results, {
        "test_0": {
            "total_pass": 1,
            "total_fail": 0
        },
        "test_1": {
            "total_pass": 0,
            "total_fail": 1
        }
    }, 0.01, None)

    assert closeEnough(result, 0.10)


def test_a_fail_b_pass():
    result = failure_rate(previous_results, {
        "test_0": {
            "total_pass": 0,
            "total_fail": 1
        },
        "test_1": {
            "total_pass": 1,
            "total_fail": 0
        }
    }, 0.01, None)

    assert closeEnough(result, 0.000917431)





def test_a_fail_b_fail():
    result = failure_rate(previous_results, {
        "test_0": {
            "total_pass": 0,
            "total_fail": 1
        },
        "test_1": {
            "total_pass": 0,
            "total_fail": 1
        }
    }, 0.01, None)

    assert closeEnough(result, 0.90)



def calc_safe(good, bad, c):
    fr=confident_fail_rate(good, bad, c)

    Log.note("range(good={{good}}, bad={{bad}}, confidence={{confidence}}) = {{safe}}", {
        "good": good,
        "bad": bad,
        "confidence": c,
        "safe": fr
    })
    return fr


def test_safe_combinations():
    assert closeEnough(calc_safe(0, 1, 0.7), 0.5)
    assert closeEnough(calc_safe(0, 1, 0.9), 0.5)
    assert closeEnough(calc_safe(0, 1, 0.95), 0.5)
    assert closeEnough(calc_safe(0, 1, 0.99), 0.5)

    assert closeEnough(calc_safe(1, 0, 0.7), 0.5)
    assert closeEnough(calc_safe(1, 0, 0.9), 0.5)
    assert closeEnough(calc_safe(1, 0, 0.95), 0.5)
    assert closeEnough(calc_safe(1, 0, 0.99), 0.5)

    assert closeEnough(calc_safe(0, 2, 0.7), 0.45227744)
    assert closeEnough(calc_safe(1, 1, 0.7), 0.5)
    assert closeEnough(calc_safe(2, 0, 0.7), 0.54772255)

    assert closeEnough(calc_safe(0, 3, 0.7), 0.3305670)
    assert closeEnough(calc_safe(1, 2, 0.7), 0.5)
    assert closeEnough(calc_safe(2, 1, 0.7), 0.5)
    assert closeEnough(calc_safe(3, 0, 0.7), 0.6694329)




def main():
    settings = startup.read_settings("test_settings.json")
    Log.start(settings.debug)
    try:
        test_a_fail_b_pass()
        test_a_pass_b_fail()
        test_a_fail_b_fail()
        test_safe_combinations()
        Log.note("SUCCESS!!")
    finally:
        Log.stop()


if __name__ == '__main__':
    main()
