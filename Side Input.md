Additional data provided to a DoFn object

- Can be provided to ParDo or transform
- Extra input(generally smaller)


```python
import apache_beam as beam

side_list=list()
with open ('exclude_ids.txt','r') as my_file:
    for line in my_file:
        side_list.append(line.rstrip())
        
#print(side_list)

p = beam.Pipeline()


# We can pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.

class FilterUsingLength(beam.DoFn):
    def process(self, element,side_list,lower_bound, upper_bound):
        id = element.split(',')[0]
        name = element.split(',')[1]
        #id=id.decode('utf-8','ignore').encode("utf-8") I am already explicitly asking for the id element[0]
        #don't need to decode
        element_list= element.split(',')
        if (lower_bound <= len(name) <= upper_bound) and id not in side_list:
            return [element_list]
        #return side_list
    

class SplitRow(beam.DoFn):
    def process(self,element):
        return [element.split(',')]

# using pardo to filter names with length between 3 and 10
small_names =(
                p
                | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
                | "ParDo with side inputs" >> beam.ParDo(FilterUsingLength(),side_list,3,10) 
                | beam.Filter(lambda record: record[3] == 'Accounts')
                | beam.Map(lambda record: (record[0]+ " " + record[1], 1))
                | beam.CombinePerKey(sum)
                | 'Write results' >> beam.io.WriteToText('data/saida')
             )

p.run()


```

    WARNING:apache_beam.io.filebasedsink:Deleting 1 existing files in target path matching: -*-of-%(num_shards)05d





    <apache_beam.runners.portability.fn_api_runner.RunnerResult at 0x7fed75d20bd0>




```python
!{('head -n 20 data/saida-00000-of-00001')}
```

    ('503996WI Edouard', 31)
    ('957149WC Kyle', 31)
    ('241316NX Kumiko', 31)
    ('796656IE Gaston', 31)
    ('718737IX Ayumi', 30)



```python
import apache_beam as beam

side_list=list()
with open ('exclude_ids.txt','r') as my_file:
    for line in my_file:
    side_list.append(line.rstrip())

p = beam.Pipeline()

# We can pass side inputs to a ParDo transform, which will get passed to its process method.
# The first two arguments for the process method would be self and element.

class FilterUsingLength(beam.DoFn):
    def process(self, element,side_list,lower_bound, upper_bound=float('inf')):
    id = element.split(',')[0]
    name = element.split(',')[1]
    id=id.decode('utf-8','ignore').encode("utf-8")
    element_list= element.split(',')
    if (lower_bound <= len(name) <= upper_bound) and id not in side_list:
        return [element_list]

# using pardo to filter names with length between 3 and 10
small_names =( 
                p
                | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
                | "ParDo with side inputs" >> beam.ParDo(FilterUsingLength(),side_list,3,10) 
                | beam.Filter(lambda record: record[3] == 'Accounts')
                | beam.Map(lambda record: (record[0]+ " " + record[1], 1))
                | beam.CombinePerKey(sum)
                | 'Write results' >> beam.io.WriteToText('data/output_new_final')
             )

p.run()

!{('head -n 20 data/output_new_final-00000-of-00001')}
```
