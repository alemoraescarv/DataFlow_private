```python
import logging
import random
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery
from apache_beam.io import WriteToText


```

## Streaming

Changing from batch to streaming in Apache Beam is quite easy, we only need to add the flag `--streaming`. 

Until now we have been run been running the code using runner `DirectRunner`, meaning the pipelines are executed in this very notebook. Since we cannot execute Streaming pipelines using that runner, now the pipelines will be executed in Dataflow using `DataflowRunner`.

Since we are going to run the pipelines outside the notebook, let's change the level of logging


```python
logging.getLogger().setLevel(logging.INFO)

```


```python
def streaming_pipeline(project_param):
    
    topic = "projects/{}/topics/beambasics".format(project_param)
    table = "{}:beam_basics.from_pubsub".format(project_param)
    schema = 'name:string,age:integer,timestamp:timestamp'
    bucket = "gs://beam-basics-{}".format(project_param)
    
    pipeline_args=["--project={}".format(project_param), "--streaming", "--experiments=allow_non_updatable_job"]
    pipeline_args.extend(["--temp_location={}/tmp/".format(bucket),"--runner=DataflowRunner", "--machine_type=n1-standard-1"])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    
    def to_dict(element):
        try:
            string = element.decode('utf-8')
            return json.loads(string)
        except ValueError:
            logging.warning("Element {} failed to load as dictionary".format(str(element)))
            pass
        
    p = beam.Pipeline(options=pipeline_options)

    pubsub = (p | "Read Topic" >> ReadFromPubSub(topic=topic)
               | "To Dict" >> beam.Map(to_dict))

    pubsub | "Write To BQ" >> WriteToBigQuery(table=table, schema=schema,
                              create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                              write_disposition=BigQueryDisposition.WRITE_APPEND)


    pubsub | "Write to GCS" >> WriteToText(bucket+"/from-pubsub/", file_name_suffix=".txt")

    p.run()

```

****

Note that there are some differences from the previous batch pipelines:
- We are adding options to the pipeline (`beam.Pipeline(options=pipeline_options)`). This is because we need to add some execution parameters, as project id, runner now is Dataflow, temporary location..
- Just by adding the `--streaming` flag, the pipeline would be streaming, no need to change any of the SDK operations
- We are not using a `With` context to handle the pipeline, so we need to add the `p.run()` at the end. This is because, similar to what the `With` does when opening files (it closes them), with pipelines it adds `p.run().wait_until_finish()`. Since it's a streaming pipeline, potentially it won't finish, so we are taking the context out. It would work fine with the context, but the cell will be running forever though


```python
#TODO
project = input("Project ID: ")
streaming_pipeline(project)

print("\n PIPELINE RUNNING \n")
```

Now we have a running streaming pipeline. Since the pipeline reads from PubSub, we would need to publish some messages.


```python
from google.cloud import pubsub_v1
import time
from datetime import datetime

M = 1000

START_UP_WAIT = 30

def publish_to_topic(N, topic, project_id):

    def publish(data, publisher):
        if isinstance(data, dict):
            data = json.dumps(data)
        data = data.encode('utf-8')
        message_future = publisher.publish(topic_path, data=data)

    names = ['joe', 'alice', 'guillem', 'rebecca', 'inigo']

    topic_name = topic

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    for i in range(N):
       time.sleep(random.random()/100)
       name = random.choice(names)
       publish({'name':name, 'age':random.randint(0,99), 'timestamp':str(datetime.now())}, publisher)

print("\nLet's wait a bit so the workers can start up \n")
time.sleep(START_UP_WAIT)
print("Ok, let's start the publishing!\n")
publish_to_topic(M, "beambasics", project)
print("\n{} Messages published".format(M))

```

    
    Let's wait a bit so the workers can start up 
    
    Ok, let's start the publishing!
    
    
    1000 Messages published


Now go to your GCP project and check how the pipeline went! You can also check the BQ table and the GCS bucket.

If you want to publish more messages, run following cell


```python
N = int(input("Messages to publish (max 10k): "))
publish_to_topic(min(N, 10000), "beambasics", project)
```

    Messages to publish (max 10k):  10000


## PLEASE, REMEMBER TO CLEAN THE RUNNING PIPELINES
