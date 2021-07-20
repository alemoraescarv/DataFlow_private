```python
!pip3 install --upgrade google-cloud-vision
```


```python
!wget https://raw.githubusercontent.com/googleapis/python-vision/master/samples/snippets/quickstart/resources/wakeupcat.jpg
```

    --2021-07-14 08:56:14--  https://raw.githubusercontent.com/googleapis/python-vision/master/samples/snippets/quickstart/resources/wakeupcat.jpg
    Resolving raw.githubusercontent.com (raw.githubusercontent.com)... 185.199.108.133, 185.199.109.133, 185.199.110.133, ...
    Connecting to raw.githubusercontent.com (raw.githubusercontent.com)|185.199.108.133|:443... connected.
    HTTP request sent, awaiting response... 200 OK
    Length: 64892 (63K) [image/jpeg]
    Saving to: ‘wakeupcat.jpg’
    
    wakeupcat.jpg       100%[===================>]  63.37K  --.-KB/s    in 0.008s  
    
    2021-07-14 08:56:14 (7.52 MB/s) - ‘wakeupcat.jpg’ saved [64892/64892]
    



```python
import io
import os

# Imports the Google Cloud client library
from google.cloud import vision

# Instantiates a client
client = vision.ImageAnnotatorClient()

# The name of the image file to annotate
file_name = os.path.abspath('wakeupcat.jpg')

# Loads the image into memory
with io.open(file_name, 'rb') as image_file:
    content = image_file.read()

image = vision.Image(content=content)

# Performs label detection on the image file
response = client.label_detection(image=image)
labels = response.label_annotations

print('Labels:')
for label in labels:
    print(label.description)
```

    Labels:
    Cat
    Window
    Felidae
    Carnivore
    Jaw
    Ear
    Small to medium-sized cats
    Window blind
    Gesture
    Whiskers



```python
def async_detect_document(gcs_source_uri, gcs_destination_uri):
    """OCR with PDF/TIFF as source files on GCS"""
    import json
    import re
    from google.cloud import vision
    from google.cloud import storage

    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # How many pages should be grouped into each json output file.
    batch_size = 2

    client = vision.ImageAnnotatorClient()

    feature = vision.Feature(
        type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.GcsSource(uri=gcs_source_uri)
    input_config = vision.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print('Waiting for the operation to finish.')
    operation.result(timeout=420)

    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.
    storage_client = storage.Client()

    match = re.match(r'gs://([^/]+)/(.+)', gcs_destination_uri)
    bucket_name = match.group(1)
    prefix = match.group(2)

    bucket = storage_client.get_bucket(bucket_name)

    # List objects with the given prefix.
    blob_list = list(bucket.list_blobs(prefix=prefix))
    print('Output files:')
    for blob in blob_list:
        print(blob.name)

    # Process the first output file from GCS.
    # Since we specified batch_size=2, the first response contains
    # the first two pages of the input file.
    output = blob_list[0]

    json_string = output.download_as_string()
    response = json.loads(json_string)

    # The actual response for the first page of the input file.
    first_page_response = response['responses'][0]
    annotation = first_page_response['fullTextAnnotation']

    # Here we print the full text from the first page.
    # The response contains more information:
    # annotation/pages/blocks/paragraphs/words/symbols
    # including confidence scores and bounding boxes
    print('Full text:\n')
    print(annotation['text'])
```


```python
gcs_source_uri='gs://cloud-samples-data/vision/pdf_tiff/census2010.pdf'
gcs_destination_uri='gs://b2-teste/visionAPI'

async_detect_document(gcs_source_uri, gcs_destination_uri)
```

    Waiting for the operation to finish.
    Output files:
    visionAPI/
    visionAPIoutput-1-to-1.json



    ---------------------------------------------------------------------------

    JSONDecodeError                           Traceback (most recent call last)

    <ipython-input-12-30b603794ebd> in <module>()
          2 gcs_destination_uri='gs://b2-teste/visionAPI'
          3 
    ----> 4 async_detect_document(gcs_source_uri, gcs_destination_uri)
    

    <ipython-input-9-c243dda9fc67> in async_detect_document(gcs_source_uri, gcs_destination_uri)
         57 
         58     json_string = output.download_as_string()
    ---> 59     response = json.loads(json_string)
         60 
         61     # The actual response for the first page of the input file.


    /opt/conda/lib/python3.7/json/__init__.py in loads(s, encoding, cls, object_hook, parse_float, parse_int, parse_constant, object_pairs_hook, **kw)
        346             parse_int is None and parse_float is None and
        347             parse_constant is None and object_pairs_hook is None and not kw):
    --> 348         return _default_decoder.decode(s)
        349     if cls is None:
        350         cls = JSONDecoder


    /opt/conda/lib/python3.7/json/decoder.py in decode(self, s, _w)
        335 
        336         """
    --> 337         obj, end = self.raw_decode(s, idx=_w(s, 0).end())
        338         end = _w(s, end).end()
        339         if end != len(s):


    /opt/conda/lib/python3.7/json/decoder.py in raw_decode(self, s, idx)
        353             obj, end = self.scan_once(s, idx)
        354         except StopIteration as err:
    --> 355             raise JSONDecodeError("Expecting value", s, err.value) from None
        356         return obj, end


    JSONDecodeError: Expecting value: line 1 column 1 (char 0)

