Creates a window with an fixed duration, when one ends another one starts. It is useful for ingesting data with a fixed duration time. In othter words, you can agrregate data within these windows.


```python
#import the packages
import apache_beam as beam

from apache_beam import Flatten, Create, ParDo, Map
from apache_beam import pvalue, window, WindowInto
from apache_beam import Keys, Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey

```


```python
with beam.Pipeline() as p:

    def key_value_fn(element, key_field):
        key = element[key_field]
        element.pop(key_field)
        return (key, element)

    elements = [
        {"user": "john", "action": "purchase", "time": 1581870000}, #16:20
        {"user": "alex", "action": "return", "time": 1581870190}, #16:23
        {"user": "john", "action": "incomplete", "time": 1581870440}, #16:27
        {"user": "phill", "action": "purchase", "time": 1581871200}, #16:40
        {"user": "phill", "action": "purchase", "time": 1581870910}, #16:35
    ]

    create = (p | "Create" >> Create(elements)
                | 'With timestamps' >> Map(lambda x: window.TimestampedValue(x, x['time']))
                | "Add keys" >> Map(key_value_fn, 'user'))

    fixed = (create | "FixedWindow" >> WindowInto(window.FixedWindows(600))  # 10 min windows
                    | "GBK Fixed" >> GroupByKey()
                    | "Print Fixed" >> Map(lambda x: print("FixedWindow: {}".format(x))))

    sliding = (create | "Window" >> WindowInto(window.SlidingWindows(600, period=300))  # 10 min windows, 5 min period
                      | "GBK Sliding" >> GroupByKey()
                      | "Print Sliding" >> Map(lambda x: print("SlidingWindow: {}".format(x))))

```

    SlidingWindow: ('john', [{'action': 'purchase', 'time': 1581870000}, {'action': 'incomplete', 'time': 1581870440}])
    SlidingWindow: ('john', [{'action': 'purchase', 'time': 1581870000}])
    SlidingWindow: ('john', [{'action': 'incomplete', 'time': 1581870440}])
    SlidingWindow: ('alex', [{'action': 'return', 'time': 1581870190}])
    SlidingWindow: ('alex', [{'action': 'return', 'time': 1581870190}])
    SlidingWindow: ('phill', [{'action': 'purchase', 'time': 1581871200}])
    SlidingWindow: ('phill', [{'action': 'purchase', 'time': 1581871200}, {'action': 'purchase', 'time': 1581870910}])
    SlidingWindow: ('phill', [{'action': 'purchase', 'time': 1581870910}])
    FixedWindow: ('john', [{'action': 'purchase', 'time': 1581870000}, {'action': 'incomplete', 'time': 1581870440}])
    FixedWindow: ('alex', [{'action': 'return', 'time': 1581870190}])
    FixedWindow: ('phill', [{'action': 'purchase', 'time': 1581871200}])
    FixedWindow: ('phill', [{'action': 'purchase', 'time': 1581870910}])

