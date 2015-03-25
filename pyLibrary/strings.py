# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

from __future__ import unicode_literals
from __future__ import division
from datetime import timedelta, date
from datetime import datetime as builtin_datetime
import re
import math
import __builtin__

from pyLibrary.dot import nvl, wrap


def datetime(value):
    from pyLibrary import convert

    if isinstance(value, (date, builtin_datetime)):
        pass
    elif value < 10000000000:
        value = convert.unix2datetime(value)
    else:
        value = convert.milli2datetime(value)

    return convert.datetime2string(value, "%Y-%m-%d %H:%M:%S")


def unix(value):
    from pyLibrary import convert

    if isinstance(value, (date, builtin_datetime)):
        pass
    elif value < 10000000000:
        value = convert.unix2datetime(value)
    else:
        value = convert.milli2datetime(value)

    return str(convert.datetime2unix(value))


def url(value):
    """
    CONVERT FROM dict OR string TO URL PARAMETERS
    """
    from pyLibrary import convert

    return convert.value2url(value)


def html(value):
    """
    CONVERT FROM unicode TO HTML OF THE SAME
    """
    from pyLibrary import convert

    return convert.unicode2HTML(value)


def upper(value):
    return value.upper()


def lower(value):
    return value.lower()


def newline(value):
    """
    ADD NEWLINE, IF SOMETHING
    """
    return "\n" + toString(value).lstrip("\n")


def replace(value, find, replace):
    return value.replace(find, replace)


def json(value):
    from pyLibrary import convert

    return convert.value2json(value)


def indent(value, prefix=u"\t", indent=None):
    if indent != None:
        prefix = prefix * indent

    value = toString(value)
    try:
        content = value.rstrip()
        suffix = value[len(content):]
        lines = content.splitlines()
        return prefix + (u"\n" + prefix).join(lines) + suffix
    except Exception, e:
        raise Exception(u"Problem with indent of value (" + e.message + u")\n" + unicode(toString(value)))


def outdent(value):
    try:
        num = 100
        lines = toString(value).splitlines()
        for l in lines:
            trim = len(l.lstrip())
            if trim > 0:
                num = min(num, len(l) - len(l.lstrip()))
        return u"\n".join([l[num:] for l in lines])
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.error("can not outdent value", e)


def round(value, decimal=None, digits=None, places=None):
    """
    :param value:  THE VALUE TO ROUND
    :param decimal: NUMBER OF DECIMAL PLACES TO ROUND (NEGATIVE IS LEFT-OF-DECIMAL)
    :param digits: ROUND TO SIGNIFICANT NUMBER OF digits
    :param places: SAME AS digits
    :return:
    """
    value = float(value)
    if value == 0.0:
        return "0"

    digits = nvl(digits, places)
    if digits != None:
        left_of_decimal = int(math.ceil(math.log10(abs(value))))
        decimal = digits - left_of_decimal

    right_of_decimal = max(decimal, 0)
    format = "{:." + unicode(right_of_decimal) + "f}"
    return format.format(__builtin__.round(value, decimal))


def percent(value, decimal=None, digits=None, places=None):
    value = float(value)
    if value == 0.0:
        return "0%"

    digits = nvl(digits, places)
    if digits != None:
        left_of_decimal = int(math.ceil(math.log10(abs(value)))) + 2
        decimal = digits - left_of_decimal

    right_of_decimal = max(decimal, 0)
    format = "{:." + unicode(right_of_decimal) + "%}"
    return format.format(__builtin__.round(value, decimal + 2))


def find(value, find, start=0):
    """
    MUCH MORE USEFUL VERSION OF string.find()
    """
    l = len(value)
    if isinstance(find, list):
        m = l
        for f in find:
            i = value.find(f, start)
            if i == -1:
                continue
            m = min(m, i)
        return m
    else:
        i = value.find(find, start)
        if i == -1:
            return l
        return i


def strip(value):
    """
    REMOVE WHITESPACE (INCLUDING CONTROL CHARACTERS)
    """
    if not value or (ord(value[0]) > 32 and ord(value[-1]) > 32):
        return value

    s = 0
    e = len(value)
    while s < e:
        if ord(value[s]) > 32:
            break
        s += 1
    else:
        return ""

    for i in reversed(range(s, e)):
        if ord(value[i]) > 32:
            return value[s:i + 1]

    return ""


def trim(value):
    return strip(value)


def between(value, prefix, suffix):
    value = toString(value)
    if prefix == None:
        e = value.find(suffix)
        if e == -1:
            return None
        else:
            return value[:e]

    s = value.find(prefix)
    if s == -1:
        return None
    s += len(prefix)

    e = value.find(suffix, s)
    if e == -1:
        return None

    s = value.rfind(prefix, 0, e) + len(prefix)  # WE KNOW THIS EXISTS, BUT THERE MAY BE A RIGHT-MORE ONE

    return value[s:e]


def right(value, len):
    if len <= 0:
        return u""
    return value[-len:]


def right_align(value, length):
    if length <= 0:
        return u""

    value = unicode(value)

    if len(value) < length:
        return (" " * (length - len(value))) + value
    else:
        return value[-length:]


def left(value, len):
    if len <= 0:
        return u""
    return value[0:len]


def comma(value):
    """
    FORMAT WITH THOUSANDS COMMA (,) SEPARATOR
    """
    try:
        if float(value) == __builtin__.round(float(value), 0):
            output = "{:,}".format(int(value))
        else:
            output = "{:,}".format(float(value))
    except Exception:
        output = unicode(value)

    return output


def quote(value):
    from pyLibrary import convert

    return convert.string2quote(value)


def split(value, sep="\n"):
    # GENERATOR VERSION OF split()
    # SOMETHING TERRIBLE HAPPENS, SOMETIMES, IN PYPY
    s = 0
    len_sep = len(sep)
    n = value.find(sep, s)
    while n > -1:
        yield value[s:n]
        s = n + len_sep
        n = value.find(sep, s)
    yield value[s:]
    value = None


def common_prefix(*args):
    prefix = args[0]
    for a in args[1:]:
        for i in range(min(len(prefix), len(a))):
            if a[i] != prefix[i]:
                prefix = prefix[:i]
                break
    return prefix


def find_first(value, find_arr, start=0):
    i = len(value)
    for f in find_arr:
        temp = value.find(f, start)
        if temp == -1: continue
        i = min(i, temp)
    if i == len(value): return -1
    return i


pattern = re.compile(r"\{\{([\w_\.]+(\|[^\}^\|]+)*)\}\}")


def expand_template(template, value):
    """
    template IS A STRING WITH {{variable_name}} INSTANCES, WHICH WILL
    BE EXPANDED TO WHAT IS IS IN THE value dict
    """
    value = wrap(value)
    if isinstance(template, basestring):
        return _simple_expand(template, (value,))

    return _expand(template, (value,))


def _expand(template, seq):
    """
    seq IS TUPLE OF OBJECTS IN PATH ORDER INTO THE DATA TREE
    """
    if isinstance(template, basestring):
        return _simple_expand(template, seq)
    elif isinstance(template, dict):
        template = wrap(template)
        assert template["from"], "Expecting template to have 'from' attribute"
        assert template.template, "Expecting template to have 'template' attribute"

        data = seq[-1][template["from"]]
        output = []
        for d in data:
            s = seq + (d,)
            output.append(_expand(template.template, s))
        return nvl(template.separator, "").join(output)
    elif isinstance(template, list):
        return "".join(_expand(t, seq) for t in template)
    else:
        from pyLibrary.debugs.logs import Log

        Log.error("can not handle")


def _simple_expand(template, seq):
    """
    seq IS TUPLE OF OBJECTS IN PATH ORDER INTO THE DATA TREE
    seq[-1] IS THE CURRENT CONTEXT
    """

    def replacer(found):
        ops = found.group(1).split("|")

        path = ops[0]
        var = path.lstrip(".")
        depth = min(len(seq), max(1, len(path) - len(var)))
        try:
            val = seq[-depth]
            if var:
                val = val[var]
            for filter in ops[1:]:
                parts = filter.split('(')
                if len(parts) > 1:
                    val = eval(parts[0] + "(val, " + ("(".join(parts[1::])))
                else:
                    val = globals()[filter](val)
            val = toString(val)
            return val
        except Exception, e:
            try:
                if e.message.find("is not JSON serializable"):
                    # WORK HARDER
                    val = toString(val)
                    return val
            except Exception, f:
                from pyLibrary.debugs.logs import Log

                Log.warning("Can not expand " + "|".join(ops) + " in template: {{template|json}}", {
                    "template": template
                }, e)
            return "[template expansion error: (" + str(e.message) + ")]"

    return pattern.sub(replacer, template)


delchars = "".join(c.decode("latin1") for c in map(chr, range(256)) if not c.decode("latin1").isalnum())


def deformat(value):
    """
    REMOVE NON-ALPHANUMERIC CHARACTERS

    FOR SOME REASON translate CAN NOT BE CALLED:
        ERROR: translate() takes exactly one argument (2 given)
	    File "C:\Python27\lib\string.py", line 493, in translate
    """
    output = []
    for c in value:
        if c in delchars:
            continue
        output.append(c)
    return "".join(output)


def toString(val):
    if val == None:
        return ""
    elif isinstance(val, (dict, list, set)):
        from pyLibrary.jsons.encoder import json_encoder

        return json_encoder(val, pretty=True)
    elif hasattr(val, "__json__"):
        return val.__json__()
    elif isinstance(val, timedelta):
        duration = val.total_seconds()
        return unicode(round(duration, 3)) + " seconds"

    try:
        return unicode(val)
    except Exception, e:
        from pyLibrary.debugs.logs import Log

        Log.error(str(type(val)) + " type can not be converted to unicode", e)


def edit_distance(s1, s2):
    """
    FROM http://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance# Python
    LICENCE http://creativecommons.org/licenses/by-sa/3.0/
    """
    if len(s1) < len(s2):
        return edit_distance(s2, s1)

    # len(s1) >= len(s2)
    if len(s2) == 0:
        return 1.0

    previous_row = xrange(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1  # j+1 instead of j since previous_row and current_row are one character longer
            deletions = current_row[j] + 1  # than s2
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1] / len(s1)


DIFF_PREFIX = re.compile(r"@@ -(\d+(?:\s*,\d+)?) \+(\d+(?:\s*,\d+)?) @@")


def apply_diff(text, diff, reverse=False):
    """
    SOME EXAMPLES OF diff
    #@@ -1 +1 @@
    #-before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    #+before china goes live (end January developer release, June general audience release) , the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    @@ -0,0 +1,3 @@
    +before china goes live, the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    +
    +kward has the details.
    @@ -1 +1 @@
    -before china goes live (end January developer release, June general audience release), the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    +before china goes live , the content team will have to manually update the settings for the china-ready apps currently in marketplace.
    @@ -3 +3 ,6 @@
    -kward has the details.+kward has the details.
    +
    +Target Release Dates :
    +https://mana.mozilla.org/wiki/display/PM/Firefox+OS+Wave+Launch+Cross+Functional+View
    +
    +Content Team Engagement & Tasks : https://appreview.etherpad.mozilla.org/40
    """
    if not diff:
        return text
    if diff[0].strip() == "":
        return text

    matches = DIFF_PREFIX.match(diff[0].strip())
    if not matches:
        from pyLibrary.debugs.logs import Log

        Log.error("Can not handle {{diff}}\n", {"diff": diff[0]})

    remove = [int(i.strip()) for i in matches.group(1).split(",")]
    if len(remove) == 1:
        remove = [remove[0], 1]  # DEFAULT 1
    add = [int(i.strip()) for i in matches.group(2).split(",")]
    if len(add) == 1:
        add = [add[0], 1]

    # UNUSUAL CASE WHERE @@ -x +x, n @@ AND FIRST LINE HAS NOT CHANGED
    half = int(len(diff[1]) / 2)
    first_half = diff[1][:half]
    last_half = diff[1][half:half * 2]
    if remove[1] == 1 and add[0] == remove[0] and first_half[1:] == last_half[1:]:
        diff[1] = first_half
        diff.insert(2, last_half)

    if not reverse:
        if remove[1] != 0:
            text = text[:remove[0] - 1] + text[remove[0] + remove[1] - 1:]
        text = text[:add[0] - 1] + [d[1:] for d in diff[1 + remove[1]:1 + remove[1] + add[1]]] + text[add[0] - 1:]
        text = apply_diff(text, diff[add[1] + remove[1] + 1:], reverse=reverse)
    else:
        text = apply_diff(text, diff[add[1] + remove[1] + 1:], reverse=reverse)
        if add[1] != 0:
            text = text[:add[0] - 1] + text[add[0] + add[1] - 1:]
        text = text[:remove[0] - 1] + [d[1:] for d in diff[1:1 + remove[1]]] + text[remove[0] - 1:]

    return text


def utf82unicode(value):
    """
    WITH EXPLANATION FOR FAILURE
    """
    try:
        return value.decode("utf8")
    except Exception, e:
        from pyLibrary.debugs.logs import Log, Except

        if not isinstance(value, basestring):
            Log.error("Can not convert {{type}} to unicode because it's not a string", {"type": type(value).__name__})

        e = Except.wrap(e)
        for i, c in enumerate(value):
            try:
                c.decode("utf8")
            except Exception, f:
                Log.error("Can not convert charcode {{c}} in string  index {{i}}", {"i": i, "c": ord(c)}, [e, Except.wrap(f)])

        try:
            latin1 = unicode(value.decode("latin1"))
            Log.error("Can not explain conversion failure, but seems to be latin1", e)
        except Exception, f:
            pass

        try:
            a = unicode(value.decode("iso-8859-1"))
            Log.error("Can not explain conversion failure, but seems to be iso-8859-1", e)
        except Exception, f:
            pass

        Log.error("Can not explain conversion failure of " + type(value).__name__ + "!", e)
