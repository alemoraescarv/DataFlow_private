```python
import apache_beam as beam
from apache_beam import bigquery
from google.cloud import bigquery

def SplitRow(element):
    return element.split(',')

p = beam.Pipeline()


input_collection = ( 
                      p 
                      | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
                      | "Split rows" >> beam.Map(SplitRow)
                   )

table_schema = 'col1:STRING, col2:INT64'
table_spec = 'bq_load_codelab.table_df'
accounts_count = (
                      input_collection
                      | 'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
                      | 'Pair each accounts employee with 1' >> beam.Map(lambda record: ("Accounts, " +record[1], 1))
                      | 'Group and sum1' >> beam.CombinePerKey(sum)
                      | 'Write results for account' >> beam.io.WriteToText('data/Account')
                      | 'Write to BQ' >>  beam.io.WriteToBigQuery(
                                                                            table_spec,
                                                                            schema=table_schema,
                                                                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

                 )

hr_count = (
                input_collection
                | 'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
                | 'Pair each hr employee with 1' >> beam.Map(lambda record: ("HR, " +record[1], 1))
                | 'Group and sum' >> beam.CombinePerKey(sum)
                | 'Write results for hr' >> beam.io.WriteToText('data/HR')
           )

output =(
         (accounts_count,hr_count)
    | beam.Flatten()
    | beam.io.WriteToText('data/both')
)



p.run()
  
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/both-00000-of-00001')}


#!{('head -n 20 data/HR-00000-of-00001')}
```


```python
!pip install --upgrade google-cloud-bigquery
```
