```python
#bq client with pandas
!pip install --upgrade google-cloud-bigquery[pandas]
!pip install --upgrade google-bigquery[pandas] --quiet
!pip install --upgrade pandas_gbq

```

    Requirement already up-to-date: google-cloud-bigquery[pandas] in /opt/conda/lib/python3.7/site-packages (1.24.0)
    Requirement already satisfied, skipping upgrade: six<2.0.0dev,>=1.13.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (1.14.0)
    Requirement already satisfied, skipping upgrade: google-cloud-core<2.0dev,>=1.1.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (1.3.0)
    Requirement already satisfied, skipping upgrade: protobuf>=3.6.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (3.11.4)
    Requirement already satisfied, skipping upgrade: google-api-core<2.0dev,>=1.15.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (1.16.0)
    Requirement already satisfied, skipping upgrade: google-auth<2.0dev,>=1.9.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (1.11.2)
    Requirement already satisfied, skipping upgrade: google-resumable-media<0.6dev,>=0.5.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (0.5.0)
    Requirement already satisfied, skipping upgrade: pandas>=0.17.1; extra == "pandas" in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery[pandas]) (1.0.3)
    Requirement already satisfied, skipping upgrade: setuptools in /opt/conda/lib/python3.7/site-packages (from protobuf>=3.6.0->google-cloud-bigquery[pandas]) (46.1.1.post20200322)
    Requirement already satisfied, skipping upgrade: requests<3.0.0dev,>=2.18.0 in /opt/conda/lib/python3.7/site-packages (from google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (2.23.0)
    Requirement already satisfied, skipping upgrade: googleapis-common-protos<2.0dev,>=1.6.0 in /opt/conda/lib/python3.7/site-packages (from google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (1.51.0)
    Requirement already satisfied, skipping upgrade: pytz in /opt/conda/lib/python3.7/site-packages (from google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (2019.3)
    Requirement already satisfied, skipping upgrade: rsa<4.1,>=3.1.4 in /opt/conda/lib/python3.7/site-packages (from google-auth<2.0dev,>=1.9.0->google-cloud-bigquery[pandas]) (4.0)
    Requirement already satisfied, skipping upgrade: pyasn1-modules>=0.2.1 in /opt/conda/lib/python3.7/site-packages (from google-auth<2.0dev,>=1.9.0->google-cloud-bigquery[pandas]) (0.2.7)
    Requirement already satisfied, skipping upgrade: cachetools<5.0,>=2.0.0 in /opt/conda/lib/python3.7/site-packages (from google-auth<2.0dev,>=1.9.0->google-cloud-bigquery[pandas]) (3.1.1)
    Requirement already satisfied, skipping upgrade: numpy>=1.13.3 in /opt/conda/lib/python3.7/site-packages (from pandas>=0.17.1; extra == "pandas"->google-cloud-bigquery[pandas]) (1.18.1)
    Requirement already satisfied, skipping upgrade: python-dateutil>=2.6.1 in /opt/conda/lib/python3.7/site-packages (from pandas>=0.17.1; extra == "pandas"->google-cloud-bigquery[pandas]) (2.8.1)
    Requirement already satisfied, skipping upgrade: urllib3!=1.25.0,!=1.25.1,<1.26,>=1.21.1 in /opt/conda/lib/python3.7/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (1.25.7)
    Requirement already satisfied, skipping upgrade: idna<3,>=2.5 in /opt/conda/lib/python3.7/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (2.9)
    Requirement already satisfied, skipping upgrade: certifi>=2017.4.17 in /opt/conda/lib/python3.7/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (2019.11.28)
    Requirement already satisfied, skipping upgrade: chardet<4,>=3.0.2 in /opt/conda/lib/python3.7/site-packages (from requests<3.0.0dev,>=2.18.0->google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery[pandas]) (3.0.4)
    Requirement already satisfied, skipping upgrade: pyasn1>=0.1.3 in /opt/conda/lib/python3.7/site-packages (from rsa<4.1,>=3.1.4->google-auth<2.0dev,>=1.9.0->google-cloud-bigquery[pandas]) (0.4.8)
    [33m  WARNING: google-bigquery 0.14 does not provide the extra 'pandas'[0m
    Requirement already up-to-date: pandas_gbq in /opt/conda/lib/python3.7/site-packages (0.13.2)
    Requirement already satisfied, skipping upgrade: setuptools in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (46.1.1.post20200322)
    Requirement already satisfied, skipping upgrade: google-auth-oauthlib in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (0.4.1)
    Requirement already satisfied, skipping upgrade: google-cloud-bigquery>=1.11.1 in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (1.24.0)
    Requirement already satisfied, skipping upgrade: pandas>=0.19.0 in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (1.0.3)
    Requirement already satisfied, skipping upgrade: google-auth in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (1.11.2)
    Requirement already satisfied, skipping upgrade: pydata-google-auth in /opt/conda/lib/python3.7/site-packages (from pandas_gbq) (0.3.0)
    Requirement already satisfied, skipping upgrade: requests-oauthlib>=0.7.0 in /opt/conda/lib/python3.7/site-packages (from google-auth-oauthlib->pandas_gbq) (1.2.0)
    Requirement already satisfied, skipping upgrade: google-cloud-core<2.0dev,>=1.1.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery>=1.11.1->pandas_gbq) (1.3.0)
    Requirement already satisfied, skipping upgrade: protobuf>=3.6.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery>=1.11.1->pandas_gbq) (3.11.4)
    Requirement already satisfied, skipping upgrade: google-api-core<2.0dev,>=1.15.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery>=1.11.1->pandas_gbq) (1.16.0)
    Requirement already satisfied, skipping upgrade: six<2.0.0dev,>=1.13.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery>=1.11.1->pandas_gbq) (1.14.0)
    Requirement already satisfied, skipping upgrade: google-resumable-media<0.6dev,>=0.5.0 in /opt/conda/lib/python3.7/site-packages (from google-cloud-bigquery>=1.11.1->pandas_gbq) (0.5.0)
    Requirement already satisfied, skipping upgrade: python-dateutil>=2.6.1 in /opt/conda/lib/python3.7/site-packages (from pandas>=0.19.0->pandas_gbq) (2.8.1)
    Requirement already satisfied, skipping upgrade: pytz>=2017.2 in /opt/conda/lib/python3.7/site-packages (from pandas>=0.19.0->pandas_gbq) (2019.3)
    Requirement already satisfied, skipping upgrade: numpy>=1.13.3 in /opt/conda/lib/python3.7/site-packages (from pandas>=0.19.0->pandas_gbq) (1.18.1)
    Requirement already satisfied, skipping upgrade: cachetools<5.0,>=2.0.0 in /opt/conda/lib/python3.7/site-packages (from google-auth->pandas_gbq) (3.1.1)
    Requirement already satisfied, skipping upgrade: rsa<4.1,>=3.1.4 in /opt/conda/lib/python3.7/site-packages (from google-auth->pandas_gbq) (4.0)
    Requirement already satisfied, skipping upgrade: pyasn1-modules>=0.2.1 in /opt/conda/lib/python3.7/site-packages (from google-auth->pandas_gbq) (0.2.7)
    Requirement already satisfied, skipping upgrade: requests>=2.0.0 in /opt/conda/lib/python3.7/site-packages (from requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (2.23.0)
    Requirement already satisfied, skipping upgrade: oauthlib>=3.0.0 in /opt/conda/lib/python3.7/site-packages (from requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (3.0.1)
    Requirement already satisfied, skipping upgrade: googleapis-common-protos<2.0dev,>=1.6.0 in /opt/conda/lib/python3.7/site-packages (from google-api-core<2.0dev,>=1.15.0->google-cloud-bigquery>=1.11.1->pandas_gbq) (1.51.0)
    Requirement already satisfied, skipping upgrade: pyasn1>=0.1.3 in /opt/conda/lib/python3.7/site-packages (from rsa<4.1,>=3.1.4->google-auth->pandas_gbq) (0.4.8)
    Requirement already satisfied, skipping upgrade: idna<3,>=2.5 in /opt/conda/lib/python3.7/site-packages (from requests>=2.0.0->requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (2.9)
    Requirement already satisfied, skipping upgrade: certifi>=2017.4.17 in /opt/conda/lib/python3.7/site-packages (from requests>=2.0.0->requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (2019.11.28)
    Requirement already satisfied, skipping upgrade: chardet<4,>=3.0.2 in /opt/conda/lib/python3.7/site-packages (from requests>=2.0.0->requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (3.0.4)
    Requirement already satisfied, skipping upgrade: urllib3!=1.25.0,!=1.25.1,<1.26,>=1.21.1 in /opt/conda/lib/python3.7/site-packages (from requests>=2.0.0->requests-oauthlib>=0.7.0->google-auth-oauthlib->pandas_gbq) (1.25.7)



```python
import pandas as pd
from google.cloud import bigquery


```


```python
import apache_beam
```


```python

```


```python
!pip show pandas_gbq
!__version__
```


```python
!pip list
```


```python

```
