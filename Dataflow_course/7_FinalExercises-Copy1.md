```python
import logging
import random
import time
import re
import json
from datetime import datetime
from google.cloud import pubsub_v1

import apache_beam as beam
from apache_beam import Create, FlatMap, Map, ParDo, Filter, Flatten, Partition
from apache_beam import Keys, Values, GroupByKey, CoGroupByKey, CombineGlobally, CombinePerKey
from apache_beam import pvalue, window, WindowInto
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.transforms.util import WithKeys
from apache_beam.transforms.combiners import Top, Mean, Count
from apache_beam.io.textio import ReadFromText, WriteToText
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

```

## Final Exercises

Here we are going to test everything we learnt during this notebooks (which I hope is a lot).

We are going to do the `hello world` of pipelines, a **WordCount**. First we would do it as usual, but after we will add a twist. We will also have a **Streaming** exercise.

As always, there are many possible solutions to this, so it's fine if your solution doesn't match ours.

### Standard WordCount

We are going to read file `kinglear.txt` and output the times each word appears. We are storing it in a file.


```python
with beam.Pipeline() as p:
    
     def split_words(text):
        words = re.split('\W+', text)
        try:
            words.remove('')
        except:
            pass
        
        return  # TODO finish function 



    # TODO: Finish the pipeline 
    (p | 
              
    
```

### Hints

**Proccess elements**
<details><summary>Hint</summary>
<p>
 
We saw before that when using `ReadFromText` we read by lines. So we need to from every line (one element) output the words (more than one element). This is clearly a `FlatMap`.
</p>
</details>


<details><summary>Code</summary>
<p>

```
    (p | "ReadTxt" >> ReadFromText(path)
       | "Split Words" >> FlatMap(split_words)
```

</p>
</details>

**Split words return**
<details><summary>Hint</summary>
<p>

In order to parse the words, we are going to use the *Regex* library. `words` is a list of the words in each text line, but this is not quite the output we need (*), we need KVs as an output so we can count them properly, our KEY would be the word, and the Value can be "1" (**)
    
(*) We don't actually need KVs in this case, at the end I will share another way of doing the same without KVs
    
(**) Depending on how we process the KVs, we need to set `1` as value or we set whatever as value. Again, another solution would be shared we each example. The solution bellow would accept whichever value.
</p>
</details>


<details><summary>Code</summary>
<p>


```
      
    def split_words(text):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass         
            return [(x, 1) for x in words]
    
```

</p>
</details>

**Full code**

<details><summary>Solution 1</summary>
<p>
    
```
with beam.Pipeline() as p:
    
    path = "Input/kinglear.txt"
    output_path = "Output/wordcount"
    
    def split_words(text):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass
            return [(x, 1) for x in words]

    (p | "ReadTxt" >> ReadFromText(path)
       | "Split Words" >> FlatMap(split_words)
       | "Count" >> Count.PerKey()
       | "Write" >> WriteToText(file_path_prefix=output_path, file_name_suffix=".txt",))
```
    

</p>
</details>
<br>

<details><summary>Solution 2</summary>
<p>
This solution doesn't require to output KVs in the FlatMap. The reason why is because it uses `Count.PerElement()` (not in the prev notebooks)

```
with beam.Pipeline() as p:
    
    path = "Input/kinglear.txt"
    output_path = "Output/wordcount"
    
    def split_words(text):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass
            return words

    (p | "ReadTxt" >> ReadFromText(path)
       | "Split Words" >> FlatMap(split_words)
       | "Count" >> Count.PerElement()
       | "Write" >> WriteToText(file_path_prefix=output_path, file_name_suffix=".txt",))
```
   
</p>
</details>

<details><summary>Solution 3</summary>
<p>
    
This solution uses the CombinePerKey rather than Count. It's a lower level solution and it can be easily modify to do other operations.

```
with beam.Pipeline() as p:
    
    path = "Input/kinglear.txt"
    output_path = "Output/wordcount"
    
    def split_words(text):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass
            return [(x, 1) for x in words]

    (p | "ReadTxt" >> ReadFromText(path)
       | "Split Words" >> FlatMap(split_words)
       | "Count" >> CombinePerKey(sum)
       | "Write" >> WriteToText(file_path_prefix=output_path, file_name_suffix=".txt",))
```
   
</p>
</details>


## Modified wordcount

Now let's spicy things up. We want to count words from two different sources: `kinglear.txt` and `hamlet.txt` but we don't want to count all words, stop words (i.e., "and", "for", "to",...). We have this stop words stored in a file, and we may add or take some words out of that file `stopwords.txt`, our pipeline has to consider this. 

Before starting coding, I recommend you to go and check the stop words file, to be able to process it

We would use as a base the prev solution using `Count.PerElement()`


```python
with beam.Pipeline() as p:
        
    # TODO: Finish pipeline

    (p |
```

### Hints

**Proccess stop words**
<details><summary>Hint</summary>
<p>

Each word in the stop word file is separated using ", ", so we can split by that. Since from one element we need more elements, we can use `FlapMap`
    
</p>
</details>


<details><summary>Code</summary>
<p>

```
    stopwords_p = (p | "Read Stop Words" >> ReadFromText(stopwords_path)
                 | FlatMap(lambda x: x.split(", "))) 
```

</p>
</details>

**Split words with condition**
<details><summary>Hint</summary>
<p>

We are in the same situation as before in which we need to use `FlatMap` (`ParDo` also works, of course). Since we have a condition, we need to use a dynamic parameter, which translates as Side Inputs. Since what we need is the list of stop words, we would use `AsList` when using the pipeline as Side Input
</p>
</details>


<details><summary>Code</summary>
<p>


```
      
    def split_words(text, stopwords):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass
            return [x for x in words if x.lower() not in stopwords]

                 <..>  | "Split Words" >> FlatMap(split_words, stopwords=beam.pvalue.AsList(stopwords_p))
    
```

</p>
</details>

**Full code**

<details><summary>Solution</summary>
<p>
    
```
with beam.Pipeline() as p:
    
    path_1 = "Input/kinglear.txt"
    path_2 = "Input/hamlet.txt"
    stopwords_path = "Input/stopwords.txt"

    output_path = "Output/modified_wordcount"
    
    def split_words(text, stopwords):
            words = re.split('\W+', text)
            try:
                words.remove('')
            except:
                pass
            return [x for x in words if x.lower() not in stopwords]
        
    stopwords_p = (p | "Read Stop Words" >> ReadFromText(stopwords_path)
                 | FlatMap(lambda x: x.split(", "))) 

    text_1 = p | "Read Text 1" >> ReadFromText(path_1)
    text_2 = p | "Read Text 2" >> ReadFromText(path_2)

    ((text_1, text_2)  | Flatten()  
                       | "Split Words" >> FlatMap(split_words, stopwords=beam.pvalue.AsList(stopwords_p))
                       | "Count" >> Count.PerElement()
                       | "Write" >> WriteToText(file_path_prefix=output_path, file_name_suffix=".txt"))

```
    

</p>
</details>



## Streaming Exercise

We're going to read from the other topic we created (`beambasics-exercise`). The structure of the messages is `name (string), money_spent (integer)` (we will parse the messages).

We need to calculate the total amount each buyer (`name`) spends every minute, and write it to BQ.

**Important note**: PubSubIO already adds the timestamp to the element (sent time), so we don't need to add the timestamp manually with `window.TimestampedValue`.


```python
def streaming_pipeline(project_param):
    
    topic = "projects/{}/topics/beambasics-exercise".format(project_param)
    bucket = "gs://beam-basics-{}".format(project_param)
    table = "{}:beam_basics.exercise".format(project_param)
    schema = 'name:string,total_spent:integer'
    
    
    pipeline_args=["--project={}".format(project_param), "--streaming", "--experiments=allow_non_updatable_job"]
    pipeline_args.extend(["--temp_location={}/tmp/".format(bucket),"--runner=DataflowRunner", "--machine_type=n1-standard-1"])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    def to_KV(element):
        try:
            string = element.decode('utf-8')
            dict = json.loads(string)
            return (dict["name"], int(dict["spent"]))
        except ValueError:
            logging.warning("Element {} failed to load as KV".format(str(element)))
            pass
        

        
    p = beam.Pipeline(options=pipeline_options)

    #TODO: Finish pipeline
    pubsub = (p | "Read Topic" >> ReadFromPubSub(topic=topic)
                | "To KV" >> beam.Map(to_KV))
    
    
 
    p.run()
```

Let's create first the publisher fn before testing the pipeline. (HINTS ARE BELLOW)


```python
M = 1000
def publish_to_topic(N, topic, project_id):

    def publish(data, publisher):
        if isinstance(data, dict):
            data = json.dumps(data)
        data = data.encode('utf-8')
        message_future = publisher.publish(topic_path, data=data)

    names = ['joe', 'alice', 'robert', 'rebecca', 'inigo']

    topic_name = topic

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    for i in range(N):
       time.sleep(random.random())
       name = random.choice(names)
       publish({'name':name, 'spent':random.randint(0,99)}, publisher)
```

To test if the pipeline works, run the following cell.

The publisher should take ~5min to finish all the messages, so take this time to check if the outputs of the pipeline are right.


```python
#TODO
project = input("Project ID: ")

streaming_pipeline(project)
print("\n PIPELINE RUNNING \n")
print("\nLet's wait a bit so the workers can start up \n")
time.sleep(30)
print("Ok, let's start the publishing!\n")
publish_to_topic(M, "beambasics-exercise", project)
print("/n PUBLISHING DONE\n")
```

### Hints

**Calculate total**
<details><summary>Hint</summary>
<p>

Since want to get the total of money spent, we need to sum the values, this is done with a `CombinePerKey`. But, since we are using streaming, we need to add Windows for Aggregations. 
    
</p>
</details>


<details><summary>Code</summary>
<p>

```
         pubsub | "FixedWindow" >> WindowInto(window.FixedWindows(60))
                | "Sum" >> CombinePerKey(sum)
```

</p>
</details>

**Write to BQ**
<details><summary>Hint</summary>
<p>

We need to do two things in order to write to BQ: prepare the elements and then actually write to BQ. When using Python, `WriteToBQ` takes either dictionaries or `TableRows` ([doc](https://beam.apache.org/releases/pydoc/2.17.0/apache_beam.io.gcp.bigquery.html?highlight=tablerow#apache_beam.io.gcp.bigquery.TableRowJsonCoder)), here we will use dictionaries. 
    
</p>
</details>


<details><summary>Code</summary>
<p>


```
      
    def prepare_for_bq(element):
        dictionary = {
            "name": element[0],
            "total_spent": element[1],
                      }
        return dictionary
        
    {..}
    
                | "Prepare for BQ" >> Map(prepare_for_bq)
                | "Write To BQ" >> WriteToBigQuery(table=table, schema=schema,
                                  create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                  write_disposition=BigQueryDisposition.WRITE_APPEND))
    
```

</p>
</details>

**Full code**

<details><summary>Solution</summary>
<p>
    
```
def streaming_pipeline(project_param):
    
    topic = "projects/{}/topics/beambasics-exercise".format(project_param)
    bucket = "gs://beam-basics-{}".format(project_param)
    table = "{}:beam_basics.exercise".format(project_param)
    schema = 'name:string,total_spent:integer'
    
    
    pipeline_args=["--project={}".format(project_param), "--streaming", "--experiments=allow_non_updatable_job"]
    pipeline_args.extend(["--temp_location={}/tmp/".format(bucket),"--runner=DataflowRunner", "--machine_type=n1-standard-1"])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    def to_KV(element):
        try:
            string = element.decode('utf-8')
            dict = json.loads(string)
            return (dict["name"], int(dict["spent"]))
        except ValueError:
            logging.warning("Element {} failed to load as KV".format(str(element)))
            pass
        
    def prepare_for_bq(element):
        dictionary = {
            "name": element[0],
            "total_spent": element[1],
                      }
        return dictionary
        
    p = beam.Pipeline(options=pipeline_options)

    pubsub = (p | "Read Topic" >> ReadFromPubSub(topic=topic)
                | "To KV" >> Map(to_KV)
                | "FixedWindow" >> WindowInto(window.FixedWindows(60))
                | "Sum" >> CombinePerKey(sum)
                | "Prepare for BQ" >> Map(prepare_for_bq)
                | "Write To BQ" >> WriteToBigQuery(table=table, schema=schema,
                                      create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                      write_disposition=BigQueryDisposition.WRITE_APPEND))
 
    p.run()

```
    

</p>
</details>



## REMEMBER TO SHUT DOWN THE PIPELINE WHEN YOU ARE DONE WITH IT


```python

```
