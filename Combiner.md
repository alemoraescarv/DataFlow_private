Combiner is a mini reducer which does the reduce task locally to a mapper machine.

List of Combiner methods

1) Create_accumulator - creates a new local accumulator in each machine. Keeps a record of (sum, counts)

2) add_input - add an input element to accumulator, returning new (sum, count) value

3) merge_accumulator - merges all machines accumulators into a single one. Ex, all the sum, counts from various machines are gathered and summed up

4) extract_output - performs the final computation on the merge_accumulation's result. Called only once on the merge_accumulator's result.


```python
import apache_beam as beam 

#need to overwritte the 4 CombineFn classes mentioned above
class AverageFn(beam.CombineFn):
    
    def create_accumulator(self):
        return (0.0, 0)   # initialize (sum, count)

    def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum + input, count + 1

    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators)       # zip - [(27, 3), (39, 3), (18, 2)]  -->   [(27,39,18), (3,3,2)]
        return sum(ind_sums), sum(ind_counts)        # (84,8)

    def extract_output(self, sum_count):    
        (sum, count) = sum_count    # combine globally using CombineFn
        return sum / count if count else float('NaN')

p=beam.Pipeline()

small_sum = (
    p
    |beam.Create([15,5,7,7,9,23,13,5])
    |beam.CombineGlobally(AverageFn())
    |'Write results' >> beam.io.WriteToText('data/combiner_ex')

)
p.run()

```

    WARNING:apache_beam.io.filebasedsink:Deleting 1 existing files in target path matching: -*-of-%(num_shards)05d





    <apache_beam.runners.portability.fn_api_runner.RunnerResult at 0x7febc818c610>




```python
# Sample the first 20 results, remember there are no ordering guarantees.
!{'head -n 20 data/combiner_ex-00000-of-00001'}
```

    10.5



```python

```
