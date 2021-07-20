### Pre Requierements and explanation

Thanks for your interest in learning Apache Beam.

In this notebooks we will cover some of the basic operations and functionalities that Apache Beam has. We are not going to focus in Dataflow itself but Apache Beam, but we would launch our streaming pipelines using that runner.

In order to improve these notebooks, we would really appreciate your feedback and we want to know if they are helpfull or not. In order to measure this, we ask you to fill a form **BEFORE** going through the notebooks and other after. This form would measure your level before and after going through the Notebooks.

[Fill this form **BEFORE** going through the notebooks](https://docs.google.com/forms/d/e/1FAIpQLSeHVo41w5hXjHFe9WDsU8NeAzXzN8UaV07TFNdMs8zQdZO8jw/viewform?usp=sf_link)

**NOTE**: These notebooks are target for people who want to learn Apache Beam. If you already know Apache Beam and want to provide feedback for them, please use [**this form**](https://docs.google.com/forms/d/e/1FAIpQLSf7wHrVrs-BU3LqW-P9iAiWhCEw8emh1Dp7LpR13LHI7FBCug/viewform?usp=sf_link) instead of the form above **AFTER** going through the Notebooks.

### Preparing for notebooks

In order to have the right resources for all the following notebooks, please run the following cells. They will:

- Create a BigQuery Dataset
- Create two topics
- Create a bucket

***TODO***

Add your PROJECT ID in the cell bellow


```bash
%%bash
export PROJECT=<PROJECT>

gcloud pubsub topics create beambasics
gcloud pubsub topics create beambasics-exercise
bq mk --dataset beam_basics
gsutil mb  gs://beam-basics-${PROJECT}

```

Let's install **Apache Beam** now (don't worry if there are some incompabilities)


```bash
%%bash
pip install apache-beam[gcp]==2.20.0 --user
pip install apache-beam[interactive]==2.20.0 --user

```


```python

```
