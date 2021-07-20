```python
#ParDo transforms eache element of input collection, performs procesing function 
#on it and emits 0,1 or multiple elements

#you can Filter, formatting, type converting, extrancting parts of element,
#computations on each element ( perform a function in each element and outputs a new Pcollection)

#you have to use DoFn
#Basics create a class and write a process logic function that inherites the beam.DoFn class
#class processingSplit(beam.DoFn):
    #def process(self,element)
        #return 
        
#parDo behaves like a map when return[something]
# pardo flatMap return .... ( you return various elemnts)
       
#below you can see we redefine SplitRow and fitlering
```

## import apache_beam as beam 

class SplitRow(beam.DoFn):
    def process(self,element):
        return [element.split(',')]
    
class FilterAccountsEmployee(beam.DoFn): 
    def process(self, element):
        if element[3] == 'Accounts':
            return [element]  
        
class PairElement(beam.DoFn):
    def process(self, element):
        return [(element[3]+','+element[1],1)]
    
class Counting(beam.DoFn):
    def process(self, element):
    # return type -> list
        (key, values) = element           # [Marco, Accounts  [1,1,1,1....] , Rebekah, Accounts [1,1,1,1,....] ]
        return [(key, sum(values))]
     

    
p1 = beam.Pipeline()

attendance_count = (
    p1
    |beam.io.ReadFromText('data_sample.txt')
    |beam.ParDo(SplitRow())
    |beam.ParDo(FilterAccountsEmployee())
    |beam.ParDo(PairElement())
    |'Group' >> beam.GroupByKey()
    |'Sum using ParDo' >> beam.ParDo(Counting())
    | beam.io.WriteToText('data/output')
    
)

p1.run()


```python
#showing the results 
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/output-00000-of-00001')}
```

    ('Accounts,Marco', 31)
    ('Accounts,Rebekah', 31)
    ('Accounts,Itoe', 31)
    ('Accounts,Edouard', 31)
    ('Accounts,Kyle', 62)
    ('Accounts,Kumiko', 31)
    ('Accounts,Gaston', 31)
    ('Accounts,Ayumi', 30)



```python

```


```python

```
