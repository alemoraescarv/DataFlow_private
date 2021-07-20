```python
import logging
import random

import apache_beam as beam
from apache_beam import  Map
from apache_beam.transforms.combiners import Count
from apache_beam.io.textio import ReadFromText, WriteToText
```

## IO Operations

In most cases, we don't use `Create` to create elements and print the elements as final destination, we use sources to read from and sinks to write to. These can be files in our system, buckets on GCS, BigQuery tables or PubSub topics.

We clasify sources in bounded or unbounded according to their size type:

- **Bounded** sources are source which we know the size of. I.e.,txt files, BQ tables, Avro Files...
- **Unbounded** sources are sources that are potentially of infinite size. Examples of this are PubSub Topic/Subscription, Kafka, DBs...

There are many IO operations ([ALL HERE](https://beam.apache.org/releases/pydoc/2.17.0/apache_beam.io.html)), in this notebook we are only going to deal reading/writing text files and we'll leave BQ and PS for the next notebook (Streaming).

**ReadFromText** reads from a file path. It can also be a GCS path. One important paremeter is `min_bundle_size` which sets the minimum bundle size of each split the source has (i.e., the file will be split in N bundles of at least `min_bundle_size`). In theory, we don't have to set this and Apache Beam should handle it, but in some scenarios we would want to maximize parelellism.

**WriteToText** reads from a file path or a GCS path. A importart paramenter here is `num_shards`,  which sets the amount of output files. Again, it is not recommended to change it, but certain use cases require a fix amount of output files.


```python
with beam.Pipeline() as p:
    
    path = "Input/example.txt"
    output_path = "Output/example"
    
    def print_fn(e):
        # We are adding this step to know how the elements look like
        print("Element: {}".format(e))
        return e
    
    (p | "Read" >> ReadFromText(path)
       | "Map" >> Map(print_fn)
       | "Write" >> WriteToText(file_path_prefix=output_path, file_name_suffix=".txt", num_shards=2))
```

    Element: this is a line of text
    Element: The second line is here!!
    Element: 
    Element: The previous one was empty :/
    Element: and we are done!


**Check the output files**. Note that the order, as in most Apache Beam operations is not kept.

We can also use wildcards as paths. Using the previous output as input:


```python
with beam.Pipeline() as p:
    output_path = "Output/example"
    
    def print_fn(e):
        # We are adding this step to know how the elements look like
        print("Element: {}".format(e))
        return e
    
    (p | "Read" >> ReadFromText(file_pattern =output_path + '*')
       | "Map" >> Map(print_fn))
```

    Element: The second line is here!!
    Element: The previous one was empty :/
    Element: this is a line of text
    Element: 
    Element: and we are done!


## Exercise

There should be a file called `hamlet.txt` in the same Input folder as before. 

We want to know how many lines that file has. Optionally you can write the number to a file, but in our solution it won't be saved.


```python
with beam.Pipeline() as p:
    
    path = "Input/hamlet.txt"
    
    # TODO: Finish the pipeline 
    (p | )
```

### Hints

<details><summary>Read Hint</summary>
<p>

When we read the file, it's already split by lines.

</p>
</details>



<details><summary><b>Code</b></summary>
<p>

```
with beam.Pipeline() as p:
    
    path = "Input/hamlet.txt"
    
    (p | ReadFromText(path)
       | Count.Globally()
       | Map(print))
```
    

</p>
</details>

