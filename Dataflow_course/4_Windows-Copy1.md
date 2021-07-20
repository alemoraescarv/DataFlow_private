```python
import logging
import random
import time
import datetime


import apache_beam as beam
from apache_beam import Create, FlatMap, Map, ParDo, Filter, Flatten, Partition
from apache_beam import Keys, Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.util import WithKeys
from apache_beam.transforms.combiners import Top, Mean, Count, MeanCombineFn
```


## Windows

Windows are a way of subdividing PCollections based on element’s timestamp and a given logic. This is needed when aggregating unbounded data, since if we don't subdivide in unbounded data into bounded data, the aggregations would not know when to finish.

By default, Apache Beam has 3 predefined ways of adding windows: FixedWindows, SlidingWindows and SessionWindows. Here we would talk about the first two. There is another type of window called GlobalWindow, which all elements fall into.

We can create our own window type, but we don't cover this here.

**FixedWindow** creates a window of a given duration and when it closes, another window is create right after it. They are good for measuring event within a given time.

![Fixed Windows](../images/fixed-time-windows.png)

**SlidingWindow** creates a window of a given duration and, after given period, it creates another overlapping window of the same duration. This means that an element can fall into more than one window. They are useful when measuring trends.

![Sliding Windows](../images/sliding-time-windows.png)



```python
with beam.Pipeline() as p:

    def key_value_fn(element, key_field):
        key = element[key_field]
        element.pop(key_field)
        return (key, element)

    elements = [
        {"user": "john", "product": "Dataflow", "time": 1581870000}, #16:20
        {"user": "rebecca", "product": "GAE", "time": 1581870180}, #16:23
        {"user": "john", "product": "BigQuery", "time": 1581870420}, #16:27
        {"user": "rebecca", "product": "CSQL", "time": 1581871200}, #16:40
        {"user": "rebecca", "product": "Spanner", "time": 1581870900}, #16:35
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

    SlidingWindow: ('rebecca', [{'time': 1581871200, 'product': 'CSQL'}])
    SlidingWindow: ('rebecca', [{'time': 1581870900, 'product': 'Spanner'}])
    SlidingWindow: ('rebecca', [{'time': 1581870180, 'product': 'GAE'}])
    SlidingWindow: ('rebecca', [{'time': 1581871200, 'product': 'CSQL'}, {'time': 1581870900, 'product': 'Spanner'}])
    SlidingWindow: ('rebecca', [{'time': 1581870180, 'product': 'GAE'}])
    SlidingWindow: ('john', [{'time': 1581870000, 'product': 'Dataflow'}])
    SlidingWindow: ('john', [{'time': 1581870420, 'product': 'BigQuery'}])
    SlidingWindow: ('john', [{'time': 1581870000, 'product': 'Dataflow'}, {'time': 1581870420, 'product': 'BigQuery'}])
    FixedWindow: ('rebecca', [{'time': 1581870180, 'product': 'GAE'}])
    FixedWindow: ('rebecca', [{'time': 1581870900, 'product': 'Spanner'}])
    FixedWindow: ('rebecca', [{'time': 1581871200, 'product': 'CSQL'}])
    FixedWindow: ('john', [{'time': 1581870000, 'product': 'Dataflow'}, {'time': 1581870420, 'product': 'BigQuery'}])


****
**Please take a good look at the output**. Note, for example, that Rebecca's Spanner case is in two SlidingWindows (window from 16:30 to 16:40 and 16:35 to 16:45), but only once in FixedWindows. 


As a side note, if we see step `With timestamps`, we see how we can modify the element metadata adding/changing the timestamp. Some operations (as PubSubIO) have it built-in, and the timestamp would be the sent-timestamp

## Exercise

We are generating elements of Players, their Score and a timestamp in Python dictionaries. We want to know the total (sum) score per player of every hour. We also need the average score of all players in the last hour, but we want it every 20 minutes.

### Important note

Since for this exercise we need windows, we cannot use the built-in global Combiners (PerKey work fine) we saw in the previous tutorial. The explanation of why is bellow the solution of the exercise. 

<details><summary>We can use this to by pass it</summary>
<p>

For `Mean/Count.Globally()` we can use
 
 ```
    CombineGlobally(MeanCombineFn()).without_defaults()
 ```
    or
  ```
    CombineGlobally(CountCombineFn()).without_defaults()
 ``` 


</p>

</details>


```python

with beam.Pipeline() as p:
    
    scores = [
        {"player":"Juan", "score":1000, "timestamp": "2020-04-30 15:35"},
        {"player":"Marina", "score":1500, "timestamp": "2020-04-30 16:10"},
        {"player":"Cristina", "score":2000, "timestamp": "2020-04-30 15:00"},
        {"player":"Cristina", "score":3500, "timestamp": "2020-04-30 15:45"},
        {"player":"Marina", "score":500, "timestamp": "2020-04-30 16:30"},
        {"player":"Juan", "score":4000, "timestamp": "2020-04-30 15:15"},
        {"player":"Cristina", "score":1000, "timestamp": "2020-04-30 16:50"},
        {"player":"Juan", "score":2000, "timestamp": "2020-04-30 16:59"},      
    ]
    
    def date2unix(string):
        unix = int(time.mktime(datetime.datetime.strptime(string, "%Y-%m-%d %H:%M").timetuple()))
        return unix

    # TODO: Finish the pipeline 
    create = (p | "Create" >> Create(scores)
                | "Add timestamps" >> Map(lambda x: window.TimestampedValue(x, date2unix(x['timestamp'])))
             )
    
```

### Hints

**Prepare elements for total per player**
<details><summary>Hint</summary>
<p>

Our input elements are dictionaries, but we are going to need KVs to process those. A `Map` function would do the work

</p>
</details>


<details><summary>Code</summary>
<p>

```
    def toKV(element):
        return (element['player'], element['score'])
    
    total = create | "To KV" >> Map(toKV) 
```

</p>
</details>

**Prepare elements for average**
<details><summary>Hint</summary>
<p>

In this case we only care about the scores, since the timestamp has been already used for the element timestamp. A `Map` function would do the work

</p>
</details>


<details><summary>Code</summary>
<p>

```
     avg =  create | "Get Score" >> Map(lambda x: x['score'])
```

</p>
</details>

**Group elements for total per player**
<details><summary>Hint</summary>
<p>

For the total per player we want to group every hour. A `FixedWindow` is the way to go.
    
</p>
</details>


<details><summary>Code</summary>
<p>

```
      | "SlidingWindow" >> WindowInto(window.SlidingWindows(60*60, period=60*20))

```

</p>
</details>

**Group elements for average**
<details><summary>Hint</summary>
<p>

In this case we want to group every hour, but get the value every 20 min, this means that there could be overlap in some elements. When we are in this situation, we need `SlidingWindows`  
</p>
</details>


<details><summary>Code</summary>
<p>

```
       | "FixedWindow" >> WindowInto(window.FixedWindows(60*60))

```

</p>
</details>

**Proccess elements for total per player**
<details><summary>Hint</summary>
<p>

We want to know the total score per player, so we are going to need a `PerKey` Combiner, in this case `CombinePerKey` with a fn that sums the values in lists  
    
</p>
</details>


<details><summary>Code</summary>
<p>

```
       | "Total Per Key" >> CombinePerKey(sum)
       | Map(lambda x: print("Total per Player: {} ".format(x)))
      
```

</p>
</details>

**Proccess elements for average**
<details><summary>Hint</summary>
<p>

This one is a bit tricker due to the note mentioned before. We are going to need a Globally combiner, since we don't care about Keys (we only have scores as elements).
</p>
</details>


<details><summary>Code</summary>
<p>

```
      | CombineGlobally(MeanCombineFn()).without_defaults()
      | "Print avg" >> Map(lambda x: print("Average: {} ".format(x)))
      
```

</p>
</details>

**Full code**
<details><summary>Code</summary>
<p>

```
with beam.Pipeline() as p:
    
    scores = [
        {"player":"Juan", "score":1000, "timestamp": "2020-04-30 15:35"},
        {"player":"Marina", "score":1500, "timestamp": "2020-04-30 16:10"},
        {"player":"Cristina", "score":2000, "timestamp": "2020-04-30 15:00"},
        {"player":"Cristina", "score":3500, "timestamp": "2020-04-30 15:45"},
        {"player":"Marina", "score":500, "timestamp": "2020-04-30 16:30"},
        {"player":"Juan", "score":4000, "timestamp": "2020-04-30 15:15"},
        {"player":"Cristina", "score":3000, "timestamp": "2020-04-30 16:50"},
        {"player":"Juan", "score":2000, "timestamp": "2020-04-30 16:59"},      
    ]
    
    def date2unix(string):
        unix = int(time.mktime(datetime.datetime.strptime(string, "%Y-%m-%d %H:%M").timetuple()))
        return unix
    
    def toKV(element):
        return (element['player'], element['score'])

    create = (p | "Create" >> Create(scores)
                | "Add timestamps" >> Map(lambda x: window.TimestampedValue(x, date2unix(x['timestamp'])))
             )
    total = (create | "To KV" >> Map(toKV) 
                   | "FixedWindow" >> WindowInto(window.FixedWindows(60*60))
                   | "Total Per Key" >> CombinePerKey(sum)
                   | Map(lambda x: print("Total per Player: {} ".format(x)))
          )
    
    avg = (create | "Get Score" >> Map(lambda x: x['score'])
                  | "SlidingWindow" >> WindowInto(window.SlidingWindows(60*60, period=60*20))
                  | CombineGlobally(MeanCombineFn()).without_defaults()
                  | "Print avg" >> Map(lambda x: print("Average: {} ".format(x)))
          )
    

</p>
</details>


## Explanation of `without_defaults`

[Beam doc](https://beam.apache.org/documentation/programming-guide/#core-beam-transforms)

When using Combine, the default Beam's behavior is to return a PCollection containing one element (this depends on the Combiner, but for example, the sum fn returns a 0). If we use Windows (apart from the Global Window) the behavior is different. Using `without_defaults` makes the output empty if the Window doesn't have elements.


```python

```
