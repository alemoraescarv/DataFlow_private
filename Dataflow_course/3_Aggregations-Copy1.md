```python
import logging
import random

import apache_beam as beam
from apache_beam import Create, Map, ParDo, Flatten
from apache_beam import Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto
from apache_beam.transforms.util import WithKeys
from apache_beam.transforms.combiners import Top, Mean, Count
```

## Aggregations

We have seen operations that happen element-wise but in order to aggregate data we need operations that happen PCollection-wise.


**GroupByKey** Takes a PCollection of KeyValue pairs and outputs each key with all values associated with that key. It's a *M(K,V) to 1(K,Vs)* per Key operation.


```python
with beam.Pipeline() as p:

    elements = [
        {"country": "China", "population": 1389, "continent": "Asia"},
        {"country": "India", "population": 1311, "continent": "Asia"},
        {"country": "USA", "population": 331, "continent": "America"},
        {"country": "Australia", "population": 25, "continent": "Oceania"},
        {"country": "Brazil", "population": 212, "continent": "America"},
    ]

    (p | "Create" >> Create(elements)
       | "Add Keys" >> WithKeys(lambda x: x['continent'])
       | GroupByKey()
       | Map(print))

```

    ('Asia', [{'population': 1311, 'country': 'India', 'continent': 'Asia'}, {'population': 1389, 'country': 'China', 'continent': 'Asia'}])
    ('America', [{'population': 212, 'country': 'Brazil', 'continent': 'America'}, {'population': 331, 'country': 'USA', 'continent': 'America'}])
    ('Oceania', [{'population': 25, 'country': 'Australia', 'continent': 'Oceania'}])


</br>

Note that the Output is the Key and a interable containing the Values.

Some of the basics combiner functions are already built-in

**Count** takes a PCollection and outputs the amount of elements  
**Top** outputs the *n* largest/smallest of a PCollection given a comparison  
**Mean** outputs the arithmetic mean of a PCollection

and we can aggreagate using the whole PCollection, by key or by element using methods

**.Globally** applies the combiner to the whole PCollection. It's a *M to 1* operation  
**.PerKey** applies the combiner for each KeyValue in the Pcollection. It's a *M(K,V) to 1(K,V)* per Key operation


```python
with beam.Pipeline() as p:
    
    def key_value_fn(element):
        return (element['continent'], element['population'])

    elements = [
        {"country": "China", "population": 1389, "continent": "Asia"},
        {"country": "India", "population": 1311, "continent": "Asia"},
        {"country": "USA", "population": 331, "continent": "America"},
        {"country": "Ireland", "population": 5, "continent": "Europe"},
        {"country": "Indonesia", "population": 273, "continent": "Asia"},
        {"country": "Brazil", "population": 212, "continent": "America"},
        {"country": "Egypt", "population": 102, "continent": "Africa"},
        {"country": "Spain", "population": 47, "continent": "Europe"},
        {"country": "Ghana", "population": 31, "continent": "Africa"},
        {"country": "Australia", "population": 25, "continent": "Oceania"},
    ]

    create = (p | "Create" >> Create(elements)
              | "map_keys" >> Map(key_value_fn))

    # Task names have to be different

    total = (create | Count.Globally()
                    | "Total print" >> Map(lambda x: print("Total elements {}".format(x))))

    top_grouped = (create | "Top" >> Top.PerKey(n=1)
                          | "Top print" >> Map(lambda x: print("Top per Key {}".format(x))))

    count_grouped = (create | "Count" >> Count.PerKey()
                            | "Count Print" >> Map(lambda x: print("Count per Key {}".format(x))))

    mean_grouped = (create | "Mean" >> Mean.PerKey()
                           | "Mean print" >> Map(lambda x: print("Mean per Key {}".format(x))))

```

    Total elements 10
    Mean per Key ('Asia', 991.0)
    Mean per Key ('Africa', 66.5)
    Mean per Key ('America', 271.5)
    Mean per Key ('Oceania', 25.0)
    Mean per Key ('Europe', 26.0)
    Top per Key ('Asia', [1389])
    Top per Key ('Africa', [102])
    Top per Key ('America', [331])
    Top per Key ('Oceania', [25])
    Top per Key ('Europe', [47])
    Count per Key ('Asia', 3)
    Count per Key ('Africa', 2)
    Count per Key ('America', 2)
    Count per Key ('Oceania', 1)
    Count per Key ('Europe', 2)


</br>

**CoGroupByKey** takes a one of more PCollections of KeyValue pairs and outputs each key with all values associated with that key for each input PCollection.   
It's a *M(K,V<sub>1</sub>), M(K,V<sub>2</sub>),.., M(K,V<sub>n</sub>) to 1(K,(V<sub>1</sub>s, V<sub>2</sub>s,.., V<sub>n</sub>s)*  per Key operation.



```python
with beam.Pipeline() as p:
    jobs = [
        ("John", "Data Scientist"),
        ("Rebecca", "Full Stack Engineer"),
        ("John", "Data Engineer"),
        ("Alice", "CEO"),
        ("Charles", "Web Designer"),
    ]

    hobbies = [
        ("John", "Baseball"),
        ("Rebecca", "Football"),
        ("John", "Piano"),
        ("Alice", "Photoshop"),
        ("Charles", "Coding"),
        ("Rebecca", "Acting"),
        ("Rebecca", "Reading")
    ]

    jobs_create = p | "Create Jobs" >> Create(jobs)
    hobbies_create = p | "Create Hobbies" >> Create(hobbies)

    ((jobs_create, hobbies_create) | CoGroupByKey()
                                   | Map(print))

```

    ('John', (['Data Scientist', 'Data Engineer'], ['Piano', 'Baseball']))
    ('Alice', (['CEO'], ['Photoshop']))
    ('Charles', (['Web Designer'], ['Coding']))
    ('Rebecca', (['Full Stack Engineer'], ['Reading', 'Football', 'Acting']))


</br>  
This operation could be thought as a Flatten+GroupByKey.

Sometimes we need to add our own logic to aggregate data, either in a global way or per key. For this, we can build our own combiners.  

**CombineGlobally** takes a PCollection and outputs the aggregated value of the given function. It's a *M to 1* operation.



```python
with beam.Pipeline() as p:

    elements = ["Lorem ipsum dolor sit amet. Consectetur adipiscing elit",
                "Sed eu velit nec sem vulputate loborti",
                "In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum",
                "Ut blandit massa et risus sollicitudin auctor"]

    (p | "Create" >> Create(elements)
       | "Join" >> CombineGlobally(lambda x: '. '.join(x))
       | Map(print))
```

    In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum. Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Ut blandit massa et risus sollicitudin auctor. Sed eu velit nec sem vulputate loborti


</br>  
Note that the order may change. We normally would want a Commutative and Associative operation, but for make this easier to understand, we used the above example.


We can also do the same operation but per key:


**CombinePerKey** takes a PCollection and outputs the aggregated value of the given function per Key It's a *M(K,V) to 1(K,V)* per Key operation.


```python
with beam.Pipeline() as p:

    elements = [
                ("Latin", "Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Sed eu velit nec sem vulputate loborti"),
                ("Latin", "In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum"),
                ("English", "From fairest creatures we desire increase"),
                ("English", "That thereby beauty's rose might never die"),
                ("English", "But as the riper should by time decease"),
                ("Spanish", "En un lugar de la Mancha, de cuyo nombre no quiero acordarme, no ha mucho"),
                ("Spanish", "tiempo que vivía un hidalgo de los de lanza en astillero, adarga antigua"),
                ]

    (p | "Create" >> Create(elements)
       | "Join By Language" >> CombinePerKey(lambda x: '. '.join(x))
       | Map(print))
```

    ('Spanish', 'tiempo que vivía un hidalgo de los de lanza en astillero, adarga antigua. En un lugar de la Mancha, de cuyo nombre no quiero acordarme, no ha mucho')
    ('Latin', 'In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum. Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Sed eu velit nec sem vulputate loborti')
    ('English', "That thereby beauty's rose might never die. But as the riper should by time decease. From fairest creatures we desire increase")


## Exercise

We are going to create KV pairs of Buyers and Items they bought. From this KVs we need to extract three things: the items each person bought, how many times each item was bought and how many total items were bought.

**Example**

From values (Bob, TV), (Alice, TV) and (Bob, Speakers) the out would be that the TV was bought 2 times and the Speakers one time, there were a total of 3 items bought and Bob bought a TV and a Speaker and Alice just a TV.


```python
with beam.Pipeline() as p:
    
    kvs = [("Bob", "TV"),
           ("Alice", "TV"),
           ("Pedro", "Speaker"),
           ("Bob", "Speaker"),
           ("Bob", "HDMI"),
           ("Alice", "Controler")]

    # TODO: Finish the pipeline 
    create = p | "Create" >> Create(kvs)
              
    
```

### Hints

**Get items per buyer**
<details><summary>Hint</summary>
<p>

We need to take the PCollection and output the grouped values per key, this needs a `GroupByKey`
    
</p>
</details>


<details><summary>Code</summary>
<p>
        

```
    gb_buyer = (create | "GBK Buyer" >> GroupByKey()
                        | "Print Buyer" >> Map(print))
```

</p>
</details>

**Count times each item was bought**
<details><summary>Hint</summary>
<p>

Since the input is the the same `create`, we just branch it out. We need to agregate the elements by key and, in this case, count them, hence we need `Count.PerKey`, but, since the input key is the buyer rather than the item, we sawp them before (there's a built-in operation but a `Map` would suffice)

</p>
</details>


<details><summary>Code</summary>
<p>


```
    items = (create | "Invert keys" >> Map(lambda x: (x[1],x[0]))
                     | "Count per key" >> Count.PerKey())
    
    print_items = items | "Print Items" >> Map(lambda x: print("Total per item {}".format(x)))
    
```

</p>
</details>

**Count total sells**
<details><summary>Hint</summary>
<p>

There is more than one way to do this, we can take the input from `create`, but that would mean each element is aggregated thrice (GBK, CountPerKey and this aggregation). A more efficient way is to sum the values that the `Count.PerKey` output (since it's already aggreagated), but just with the Values of the KVs. Since we don't need to aggregate considering the Key (there are no keys now), we can use `Combine.Globally`
    
</p>
</details>


<details><summary>Code</summary>
<p>


```
    total = (items | Values()
                   | CombineGlobally(sum)
                   | "Print Total" >> Map(lambda x: print("Total items {}".format(x))))
    
```

</p>
</details>

**Full code**
<details><summary>Code</summary>
<p>

```
with beam.Pipeline() as p:
    
    kvs = [("Bob", "TV"),
           ("Alice", "TV"),
           ("Pedro", "Speaker"),
           ("Bob", "Speaker"),
           ("Bob", "HDMI"),
           ("Alice", "Controler")]
        

    # TODO: Finish the pipeline 
    create = p | "Create" >> Create(kvs)
    
    gb_buyer = ( create | "GBK Buyer" >> GroupByKey()
                        | "Print Buyer" >> Map(print))
    
    items = (create | "Invert keys" >> Map(lambda x: (x[1],x[0]))
                     | "Count per key" >> Count.PerKey())
    
    print_items = items | "Print Items" >> Map(lambda x: print("Total per item {}".format(x)))
    
    total = (items | Values()
                   | CombineGlobally(sum)
                   | "Print Total" >> Map(lambda x: print("Total items {}".format(x))))
```
    

</p>
</details>



```python

```
