
Consistent dicts, lists and Nones
=================================

Struct replaces dict
--------------------

```Struct``` is used to declare an instance of an anonymous type, and has good
features for manipulating JSON.  Anonymous types are necessary when
writing sophisticated list comprehensions, or queries, and to keep them
readable.  In many ways, dict() can act as an anonymous type, but it does
not have the features listed here.

 1. ```a.b == a["b"]```
 2. missing keys are handled gracefully, which is beneficial when being used in set operations (database operations) without raising exceptions

        ```python
        a = wrap({})
        >>> a == {}
        a.b == None
        >>> True
        a.b.c == None
        >>> True
        a[None] == None
        >>> True
        ```

    missing keys are common when dealing with JSON, which is often almost anything.  Unfortunalty, you do loose the ability to perform <code>a is None</code> checks:  You must always use <code>a == None</code> instead.

 3. remove an attribute by assigning ```None```:

        ```python
            a.b = None
        ```

 4. you can access paths as a variable:  ```a["b.c"] == a.b.c```
 5. you can set paths to values, missing dicts along the path are created:

        ```python
            a = wrap({})<br>
            > a == {}<br>
            a["b.c"] = 42<br>
            > a == {"b": {"c": 42}}
        ```

 6. attribute names (keys) are corrected to unicode - it appears Python
   object.getattribute() is called with str() even when using
        ```python
            from __future__ import unicode_literals
        ```

7. by allowing dot notation, the IDE does tab completion and my spelling
   mistakes get found at "compile time"

### Examples ###

```Struct``` is a common pattern in many frameworks even though it goes by
different names, some examples are:

 * jinja2.environment.Environment.getattr()
 * argparse.Environment() - code performs setattr(e, name, value) on
  instances of Environment to provide dot(.) accessors
 * collections.namedtuple() - gives attribute names to tuple indicies
  effectively providing <code>a.b</code> rather than <code>a["b"]</code>
     offered by dicts
 * C# Linq requires anonymous types to avoid large amounts of boilerplate code.
 * D3 has many of these conventions ["The function's return value is
  then used to set each element's attribute. A null value will remove the
  specified attribute."](https://github.com/mbostock/d3/wiki/Selections#attr)

### Notes ###
 * More on missing values: [http://www.np.org/NA-overview.html](http://www.np.org/NA-overview.html) it only considers the legitimate-field-with-missing-value (Statistical Null) and does not look at field-does-not-exist-in-this-context (Database Null)
 * [Motivation for a 'mutable named tuple'](http://www.saltycrane.com/blog/2012/08/python-data-object-motivated-desire-mutable-namedtuple-default-values/) (aka anonymous class)

Null instead of None
--------------------






Slicing is Broken in Python 2.7
-------------------------------

###The slice operator in Python2.7 is inconsistent###

At first glance, the python slice operator ```[:]``` is elegant and powerful.
Unfortunately it is inconsistent and forces the programmer to write extra code
to work around these inconsistencies.

```python
    my_list = ['a', 'b', 'c', 'd', 'e']
```

Let us iterate through some slices:

```python
    my_list[4:] == ['e']
    my_list[3:] == ['d', 'e']
    my_list[2:] == ['c', 'd', 'e']
    my_list[1:] == ['b', 'c', 'd', 'e']
    my_list[0:] == ['a', 'b', 'c', 'd', 'e']
```

Looks good, but this time let's use negative indices:

```python
    my_list[-4:] == ['b', 'c', 'd', 'e']
    my_list[-3:] == ['c', 'd', 'e']
    my_list[-2:] == ['d', 'e']
    my_list[-1:] == ['e']
    my_list[-0:] == ['a', 'b', 'c', 'd', 'e']  # [] is expected
```

Using negative idiocies ```[-num:]``` allows the programmer to slice relative to
the right rather than the left.  When ```num``` is a constant this problem is
never revealed, but when ```num``` is a variable, then the inconsistency can
reveal itself.

```python
    def get_suffix(num):
        return my_list[-num:]   # wrong
```

So, clearly, ```[-num:]``` can not be understood as a suffix slice, rather
something more complicated; especially considering that ```num``` could be
negative.

I advocate never using negative indices in the slice operator.  Rather, use the
```right()``` method instead which is consistent for a range ```num```:

```python
    def right(_list, num):
        if num <= 0:
            return []
        return _list[-num:]
```

###Python 2.7 ```__getslice__``` is broken###

It would be nice to have our own list-like class that implements slicing in a
way that is consistent.  Specifically, we expect to solve the inconsistent
behaviour seen when dealing with negative indices.

As an example, I would like to ensure my over-sliced-to-the-right and over-
sliced-to-the-left  behave the same.  Let's look at over-slicing-to-the-right,
which behaves as expected on a regular list:

```python
    assert 3 == len(my_list[1:4])
    assert 4 == len(my_list[1:5])
    assert 4 == len(my_list[1:6])
    assert 4 == len(my_list[1:7])
    assert 4 == len(my_list[1:8])
    assert 4 == len(my_list[1:9])
```

Any slice that requests indices past the list's length is simply truncated.
I would like to implement the same for over-slicing-to-the-left:

```python
    assert 2 == len(my_list[ 1:3])
    assert 3 == len(my_list[ 0:3])
    assert 3 == len(my_list[-1:3])
    assert 3 == len(my_list[-2:3])
    assert 3 == len(my_list[-3:3])
    assert 3 == len(my_list[-4:3])
    assert 3 == len(my_list[-5:3])
```

Here is an attempt:

```python
    class MyList(list):
        def __init__(self, value):
            self.list = value

        def __getslice__(self, i, j):
            if i < 0:  # CLAMP i TO A REASONABLE RANGE
                i = 0
            elif i > len(self.list):
                i = len(self.list)

            if j < 0:  # CLAMP j TO A REASONABLE RANGE
                j = 0
            elif j > len(self.list):
                j = len(self.list)

            if i > j:  # DO NOT ALLOW THE IMPOSSIBLE
                i = j

            return [self.list[index] for index in range(i, j)]

        def __len__(self):
            return len(self.list)
```

Unfortunately this does not work.  When the ```__len__``` method is defined:
```__getslice__``` defines ```i = i % len(self)```: Which
makes it impossible to identify if a negative value is passed to the slice
operator.

The solution is to implement Python's extended slice operator ```[::]```,
which can be implemented using ```__getitem__```; it does not suffer from this
wrap-around problem.

```python
    class BetterList(list):
        def __init__(self, value):
            self.list = value

        def __getslice__(self, i, j):
            raise NotImplementedError

        def __len__(self):
            return len(self.list)

        def __getitem__(self, item):
            if not isinstance(item, slice):
                # ADD [] CODE HERE

            i = item.start
            j = item.stop
            k = item.step

            if i < 0:  # CLAMP i TO A REASONABLE RANGE
                i = 0
            elif i > len(self.list):
                i = len(self.list)

            if j < 0:  # CLAMP j TO A REASONABLE RANGE
                j = 0
            elif j > len(self.list):
                j = len(self.list)

            if i > j:  # DO NOT ALLOW THE IMPOSSIBLE
                i = j

            return [self.list[index] for index in range(i, j)]
```

[StructList](https://github.com/klahnakoski/pyLibrary/blob/master/pyLibrary/struct.py)
implements slicing this way.
