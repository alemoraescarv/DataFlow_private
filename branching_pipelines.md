```python
#Pipeline linear flow of operations , one to one mapping. 
#MapTransfor -> MapTransform etc..
#easy pipeline
```


```python
#Branching is when read from file, get a pCollection and then transform it in two new pCollections.
#And two outputs 
```


```python
!{'pip install  apache_beam'}
```


```python
import apache_beam as beam
from apache_beam import typehints
from apache_beam import io
from apache_beam import coders

def SplitRow(element):
    return element.split(',')

p = beam.Pipeline()


input_collection = ( 
                      p 
                      | "Read from text file" >> beam.io.ReadFromText('data_sample.txt')
                      | "Split rows" >> beam.Map(SplitRow)
                   )

accounts_count = (
                      input_collection
                      | 'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
                      | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ("Accounts, " +record[1], 1))
                      | 'Group and sum1' >> beam.CombinePerKey(sum)
                      | 'Write results for account' >> beam.io.WriteToText('data/Account')
                 )

hr_count = (
                input_collection
                | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
                | 'Pair each hr employee with 1' >> beam.Map(lambda record: ("HR, " +record[1], 1))
                | 'Group and sum' >> beam.CombinePerKey(sum)
                #| 'Write results for hr' >> beam.io.WriteToText('data/HR')
           )

output =(
         (accounts_count,hr_count)
    | beam.Flatten()
    | beam.io.WriteToText('data/both1')
)



p.run()
  
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/both1-00000-of-00001')}


#!{('head -n 20 data/HR-00000-of-00001')}

```


    ---------------------------------------------------------------------------

    AttributeError                            Traceback (most recent call last)

    <ipython-input-8-09a5c01c9998> in <module>
    ----> 1 import apache_beam as beam
          2 from apache_beam import typehints
          3 from apache_beam import io
          4 from apache_beam import coders
          5 


    /opt/conda/lib/python3.7/site-packages/apache_beam/__init__.py in <module>
         95 import apache_beam.internal.pickler
         96 
    ---> 97 from apache_beam import coders
         98 from apache_beam import io
         99 from apache_beam import typehints


    /opt/conda/lib/python3.7/site-packages/apache_beam/coders/__init__.py in <module>
         17 from __future__ import absolute_import
         18 
    ---> 19 from apache_beam.coders.coders import *
         20 from apache_beam.coders.row_coder import *
         21 from apache_beam.coders.typecoders import registry


    /opt/conda/lib/python3.7/site-packages/apache_beam/coders/coders.py in <module>
         44 from past.builtins import unicode
         45 
    ---> 46 from apache_beam.coders import coder_impl
         47 from apache_beam.coders.avro_record import AvroRecord
         48 from apache_beam.portability import common_urns


    /opt/conda/lib/python3.7/site-packages/apache_beam/coders/coder_impl.cpython-37m-x86_64-linux-gnu.so in init apache_beam.coders.coder_impl()


    AttributeError: type object 'apache_beam.coders.coder_impl.CoderImpl' has no attribute '__reduce_cython__'



```python

```


```python

```


```python
#view the head of files
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/both-00000-of-00001')}


#!{('head -n 20 data/HR-00000-of-00001')}
```


```python

```
