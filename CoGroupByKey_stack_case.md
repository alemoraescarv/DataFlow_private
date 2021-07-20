```python
#import the packages
import apache_beam as beam

from apache_beam import Flatten, Create, ParDo, Map

```


```python

```


```python

```


```python
p = beam.Pipeline()


people_info=[
    (1,"Leo",30,"NYC"),
    (2,"Ralph",25,"Sydney"),
#    ("ID":3,"Name": "Mary", "Age": 27,"City": "NYC"),
#    ("ID":4,"Name": "Phillip", "Age": 50,"City": "Paris"),
#    ("ID":5,"Name": "Diana", "Age": 19,"City": "NYC")
]

address = [    
    (1,"5th Av. 32"),
    (2,"George St. 100"),
#    ("ID":3,"Address": "Pyrmont St. 55"),
#    ("ID":4,"Address": "Marina Bay 1"),
#    ("ID":5,"Address": "5th Av. 50")
]

#first create your PCollection
#pcollection = (
#    p
#    |"Creating the PCollection" >> Create(elements)
    #|"Map" >> SplitRow(elements)
    #|ParDo(print)
#)

#filter your first new PCollection
people_under_30 = (
    people_info
    |Create(people_info)
    |"Filter" >> beam.Filter(lambda x:x(3))
    #|"Map" >> beam.Map(lambda x:x["Name"])
    #|"Print" >> ParDo(print)
)





p.run()

```


```python
p = beam.Pipeline()

def SplitRow(element):
    split_list =[element.split(',') for i in element]
    return split_list

elements=[
    {1,"Leo",30,"NYC"},
    {2,"Ralph",25,"Sydney"},
    {3,"Mary",27,"NYC"},
    {4,"Phillip",50,"Paris"},
    {5,"Diana",19,"NYC"}
]

pcollection = (
    p
    #|"Creating the PCollection" >> Create(elements)
    | "Split rows" >> beam.Map(SplitRow(elements))
    #|"Map" >> SplitRow(elements)
    |ParDo(print)
)

p.run()

```


    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    <ipython-input-4-00ec31338455> in <module>()
         18     | "Split rows" >> beam.Map(SplitRow(elements))
         19     #|"Map" >> SplitRow(elements)
    ---> 20     |ParDo(print)
         21 )
         22 


    <ipython-input-4-00ec31338455> in SplitRow(element)
          2 
          3 def SplitRow(element):
    ----> 4     split_list =[element.split(',') for i in element]
          5     return split_list
          6 


    <ipython-input-4-00ec31338455> in <listcomp>(.0)
          2 
          3 def SplitRow(element):
    ----> 4     split_list =[element.split(',') for i in element]
          5     return split_list
          6 


    AttributeError: 'list' object has no attribute 'split'



```python
p = beam.Pipeline()

adress_list = [
    ('leo', 'George St. 32'),
    ('ralph', 'Pyrmont St. 30'),
    ('mary', '10th Av.'),
    ('carly', 'Marina Bay 1'),
]
city_list = [
    ('leo', 'Sydney'),
    ('ralph', 'Sydney'),
    ('mary', 'NYC'),
    ('carly', 'Brisbane'),
]

street = p | 'CreateEmails' >> beam.Create(adress_list)
city = p | 'CreatePhones' >> beam.Create(city_list)

results = (
    (street, city)
    | beam.CoGroupByKey()
    |ParDo(print)
    #| beam.io.WriteToText('delete.txt')
    
)

p.run()





```


```python

```


```python

```
