```python
import logging
import random
import sympy

import apache_beam as beam
from apache_beam import Create, FlatMap, Map, ParDo, Filter, Flatten, Partition
from apache_beam import Keys, Values
from apache_beam.transforms.util import WithKeys

```

## Element-wise operations

We have seen two ways of doing operations at element level: **ParDo** and **Map**. **ParDo** as the most general operation (*1 to (0,1,M)*) and **Map** as a simplification for *1 to 1* operations. While these two operations would suffice (actually, just with ParDo we can do everything), there are some other **element-wise** operations that would help with readability and optimization.

**Filter** applies a function for every element and outputs it if the function returns *True*. It's a *1 to (0,1)* operation.


```python
with beam.Pipeline() as p:
    N = 20
    
    (p | "CreateNumbers" >> Create(range(N))
       | "IsPrime" >> Filter(sympy.isprime)
       | Map(print))
```

    19
    3
    5
    13
    11
    17
    7
    2


**FlatMap** applies a transformation to an elements and outputs none, one or more elements. High level transformation. It's a simplification of the **ParDo** for *1 to (0,1,M)* operations.


```python
with beam.Pipeline() as p:

    elements = ["Lorem ipsum dolor sit amet. Consectetur adipiscing elit. Sed eu velit nec sem vulputate loborti",
                "In lobortis augue vitae sagittis molestie. Mauris volutpat tortor non purus elementum",
                "Ut blandit massa et risus sollicitudin auctor"]

    (p | Create(elements)
       | FlatMap(lambda x: x.split('. '))
       | Map(print))
```

    In lobortis augue vitae sagittis molestie
    Mauris volutpat tortor non purus elementum
    Lorem ipsum dolor sit amet
    Consectetur adipiscing elit
    Sed eu velit nec sem vulputate loborti
    Ut blandit massa et risus sollicitudin auctor


</br>

We can use PCollections as parameters in our functions as Side Inputs in the **ParDo** operation. This parameter can be treated as a dictionary, as a list, as singletons...




```python
with beam.Pipeline() as p:

    elements = [
        {"currency": "USD", "amount": 2.728281, "change_to": "CHF"},
        {"currency": "EUR", "amount": 3.141592, "change_to": "USD"},
        {"currency": "CHF", "amount": 1729, "change_to": "EUR"},
    ]
    
    eur = {"CHF":1.0585,"EUR":1, "USD":1.0956}
    usd = {"CHF":0.9661372764,"EUR":0.9127418766,"USD":1}
    chf = {"EUR":0.9447331129,"CHF":1,"USD":1.0350495985}
    rates = {"EUR":eur, "USD":usd, "CHF":chf}
    
    
    def change_currency(value, ratios):
        current = value['currency']
        desired = value['change_to']
        amount = value['amount']
        changed = amount * ratios[current][desired]

        return ["{} {} are {} {}".format(amount,current,changed,desired)]

    exchange = p | "Rates" >> Create(rates)

    (p | Create(elements)
       | beam.ParDo(change_currency, ratios=beam.pvalue.AsDict(exchange))
       | "Print" >> Map(print))
```

    3.141592 EUR are 3.4419281952 USD
    2.728281 USD are 2.6358939745938685 CHF
    1729 CHF are 1633.4435522041 EUR


 

One of the fundamentals of all ETL frameworks is the use of KeyValue pairs for aggreating and/or grouping data according to some logic. Apache Beam has some built-in operations to add/extract Keys and Values.

**WithKeys** adds a key to each element and outputs the Key and old element. *1 to 1(K, V)* operation.

**Keys** outputs the Key of a KeyValue pair. *1(K, V) to K* operation.

**Values** outputs the Value of a KeyValue pair. *1(K,V) to V* operation.


```python
with beam.Pipeline() as p:

    elements = [
        {"country": "China", "population": 1389, "continent": "Asia"},
        {"country": "India", "population": 1311, "continent": "Asia"},
        {"country": "USA", "population": 331, "continent": "America"},
        {"country": "Australia", "population": 25, "continent": "Oceania"},
        {"country": "Brazil", "population": 212, "continent": "America"},
    ]

    create = (p | "Create" >> Create(elements)
                | WithKeys(lambda x: x['continent']))

    key_print = (create | Keys()
                        | "print_keys" >> Map(lambda x: print("Key {}".format(x))))

    value_print = (create | Values()
                          | "print_values" >> Map(lambda x: print("Value {}".format(x))))
```

    Value {'continent': 'Oceania', 'country': 'Australia', 'population': 25}
    Key Oceania
    Value {'continent': 'America', 'country': 'Brazil', 'population': 212}
    Key America
    Value {'continent': 'Asia', 'country': 'India', 'population': 1311}
    Key Asia
    Value {'continent': 'Asia', 'country': 'China', 'population': 1389}
    Key Asia
    Value {'continent': 'America', 'country': 'USA', 'population': 331}
    Key America


## Exercise

We are going to create N lists of numbers. For every number within the list, we are going to eliminate it if it's even

As an example:  **element** [5, 24, 10, 13, 1] should return **elementS** 5, 13, 1


There are hints bellow and the solution at the end.


```python
with beam.Pipeline() as p:
    
    elements = [[1, 6, 29, 17],
                [4, 7, 1729, 3],
                [3.1415]]

    # TODO: Finish the pipeline 
    (p | "Create" >> Create(elements)
              
    
```

### Hints

**Proccess created elements**
<details><summary>Hint</summary>
<p>

Since from one element (the list) we need to output 1 or more elements, we need to use a `FlatMap` or `ParDo`

</p>
</details>


<details><summary>Code</summary>
<p>

   Every element is an iterable, so we can just return the iterable from the `FlatMap`
    
    

```
        create = (p | "Create" >> Create(elements)
                    | "Flatmap" >> FlatMap(lambda x: x))
```

</p>
</details>

**Eliminate elements given according to a rule**
<details><summary>Hint</summary>
<p>

We need to filter the elements by odd or even, so we can use `Filter` (as always, we can use the general `ParDo` or even `Map`)

</p>
</details>


<details><summary>Code</summary>
<p>


```
      
     create | Filter(lambda x: x%2==1)
    
```

</p>
</details>

**Full code**
<details><summary>Code</summary>
<p>

```
with beam.Pipeline() as p:
    
    elements = [[1, 6, 29, 17],
                [4, 7, 1729, 3],
                [3.1415]]

    # TODO: Finish the pipeline 
    (p | "Create" >> Create(elements)
       | FlatMap(lambda x: x)
       | Filter(lambda x: x%2==1)
       | Map(print))
```
    

</p>
</details>



```python

```
