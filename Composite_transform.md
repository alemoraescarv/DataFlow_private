Combine multiple types of transform and combine into one
Used: 
CombinePerKey
Filter
Map

Transform to: one func

- PTransform is a base class of every transform that it is performed
- we have to redefine expand func



```python
import apache_beam as beam
  
class MyTransform(beam.PTransform):
    def expand(self, input_coll):
        a = ( 
            input_coll
            | 'Group and sum1' >> beam.CombinePerKey(sum)
            | 'count filter accounts' >> beam.Filter(filter_on_count)
            | 'Regular accounts employee' >> beam.Map(format_output)     
        )
        return a
    
def SplitRow(element):
    return element.split(',')

def filter_on_count(element):
    name, count = element
    if count > 30:
        return element
    
def format_output(element):
    name, count = element
    return ', '.join((name.encode('ascii'),str(count),'Regular employee'))

p = beam.Pipeline()

input_collection = ( 
                    p 
                    |"Read from text file" >> beam.io.ReadFromText('data_sample.txt')
                    |"Split rows" >> beam.Map(SplitRow)
                   )

accounts_count = (
                      input_collection
                      |'Get all Accounts dept persons' >> beam.Filter(lambda record: record[3] == 'Accounts')
                      |'Pair each accounts employee with 1' >> beam.Map(lambda record: ("Accounts, " +record[1], 1))
                      |'composite accoubts' >> MyTransform()
                      |'Write results for account' >> beam.io.WriteToText('data/Account')
                 )
hr_count = (
                input_collection
                |'Get all HR dept persons' >> beam.Filter(lambda record: record[3] == 'HR')
                |'Pair each hr employee with 1' >> beam.Map(lambda record: ("HR, " +record[1], 1))
                |'composite HR' >> MyTransform()
                |'Write results for hr' >> beam.io.WriteToText('data/HR')
           ) 
p.run()
  
```


    ---------------------------------------------------------------------------

    TypeError                                 Traceback (most recent call last)

    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.SimpleInvoker.invoke_process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/transforms/core.py in <lambda>(x)
       1436   else:
    -> 1437     wrapper = lambda x: [fn(x)]
       1438 


    <ipython-input-39-d51443206d2e> in format_output(element)
         22     name, count = element
    ---> 23     return ', '.join((name.encode('ascii'),str(count),'Regular employee'))
         24 


    TypeError: sequence item 0: expected str instance, bytes found

    
    During handling of the above exception, another exception occurred:


    TypeError                                 Traceback (most recent call last)

    <ipython-input-39-d51443206d2e> in <module>
         45                 |'Write results for hr' >> beam.io.WriteToText('data/HR')
         46            ) 
    ---> 47 p.run()
         48 


    /opt/conda/lib/python3.7/site-packages/apache_beam/pipeline.py in run(self, test_runner_api)
        459           self.to_runner_api(use_fake_coders=True),
        460           self.runner,
    --> 461           self._options).run(False)
        462 
        463     if self._options.view_as(TypeOptions).runtime_type_check:


    /opt/conda/lib/python3.7/site-packages/apache_beam/pipeline.py in run(self, test_runner_api)
        472       finally:
        473         shutil.rmtree(tmpdir)
    --> 474     return self.runner.run_pipeline(self, self._options)
        475 
        476   def __enter__(self):


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/direct/direct_runner.py in run_pipeline(self, pipeline, options)
        180       runner = BundleBasedDirectRunner()
        181 
    --> 182     return runner.run_pipeline(pipeline, options)
        183 
        184 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_pipeline(self, pipeline, options)
        484 
        485     self._latest_run_result = self.run_via_runner_api(pipeline.to_runner_api(
    --> 486         default_environment=self._default_environment))
        487     return self._latest_run_result
        488 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_via_runner_api(self, pipeline_proto)
        492     # TODO(pabloem, BEAM-7514): Create a watermark manager (that has access to
        493     #   the teststream (if any), and all the stages).
    --> 494     return self.run_stages(stage_context, stages)
        495 
        496   @contextlib.contextmanager


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in run_stages(self, stage_context, stages)
        581               stage,
        582               pcoll_buffers,
    --> 583               stage_context.safe_coders)
        584           metrics_by_stage[stage.name] = stage_results.process_bundle.metrics
        585           monitoring_infos_by_stage[stage.name] = (


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in _run_stage(self, worker_handler_factory, pipeline_components, stage, pcoll_buffers, safe_coders)
        902         cache_token_generator=cache_token_generator)
        903 
    --> 904     result, splits = bundle_manager.process_bundle(data_input, data_output)
        905 
        906     def input_for(transform_id, input_id):


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in process_bundle(self, inputs, expected_outputs)
       2103 
       2104     with UnboundedThreadPoolExecutor() as executor:
    -> 2105       for result, split_result in executor.map(execute, part_inputs):
       2106 
       2107         split_result_list += split_result


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in result_iterator()
        596                     # Careful not to keep a reference to the popped future
        597                     if timeout is None:
    --> 598                         yield fs.pop().result()
        599                     else:
        600                         yield fs.pop().result(end_time - time.monotonic())


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in result(self, timeout)
        433                 raise CancelledError()
        434             elif self._state == FINISHED:
    --> 435                 return self.__get_result()
        436             else:
        437                 raise TimeoutError()


    /opt/conda/lib/python3.7/concurrent/futures/_base.py in __get_result(self)
        382     def __get_result(self):
        383         if self._exception:
    --> 384             raise self._exception
        385         else:
        386             return self._result


    /opt/conda/lib/python3.7/site-packages/apache_beam/utils/thread_pool_executor.py in run(self)
         42       # If the future wasn't cancelled, then attempt to execute it.
         43       try:
    ---> 44         self._future.set_result(self._fn(*self._fn_args, **self._fn_kwargs))
         45       except BaseException as exc:
         46         # Even though Python 2 futures library has #set_exection(),


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in execute(part_map)
       2100           self._progress_frequency, self._registered,
       2101           cache_token_generator=self._cache_token_generator)
    -> 2102       return bundle_manager.process_bundle(part_map, expected_outputs)
       2103 
       2104     with UnboundedThreadPoolExecutor() as executor:


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in process_bundle(self, inputs, expected_outputs)
       2023             process_bundle_descriptor_id=self._bundle_descriptor.id,
       2024             cache_tokens=[next(self._cache_token_generator)]))
    -> 2025     result_future = self._worker_handler.control_conn.push(process_bundle_req)
       2026 
       2027     split_results = []  # type: List[beam_fn_api_pb2.ProcessBundleSplitResponse]


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/portability/fn_api_runner.py in push(self, request)
       1356       self._uid_counter += 1
       1357       request.instruction_id = 'control_%s' % self._uid_counter
    -> 1358     response = self.worker.do_instruction(request)
       1359     return ControlFuture(request.instruction_id, response)
       1360 


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/sdk_worker.py in do_instruction(self, request)
        350       # E.g. if register is set, this will call self.register(request.register))
        351       return getattr(self, request_type)(getattr(request, request_type),
    --> 352                                          request.instruction_id)
        353     else:
        354       raise NotImplementedError


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/sdk_worker.py in process_bundle(self, request, instruction_id)
        384         with self.maybe_profile(instruction_id):
        385           delayed_applications, requests_finalization = (
    --> 386               bundle_processor.process_bundle(instruction_id))
        387           monitoring_infos = bundle_processor.monitoring_infos()
        388           monitoring_infos.extend(self.state_cache_metrics_fn())


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/bundle_processor.py in process_bundle(self, instruction_id)
        810             instruction_id, expected_transforms):
        811           input_op_by_transform_id[
    --> 812               data.transform_id].process_encoded(data.data)
        813 
        814       # Finish all operations.


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/bundle_processor.py in process_encoded(self, encoded_windowed_values)
        203       decoded_value = self.windowed_coder_impl.decode_from_stream(
        204           input_stream, True)
    --> 205       self.output(decoded_value)
        206 
        207   def try_split(self, fraction_of_remainder, total_buffer_size):


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.Operation.output()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.Operation.output()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.SingletonConsumerSet.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.CombineOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.CombineOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.Operation.output()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.SingletonConsumerSet.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.CombineOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.CombineOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.Operation.output()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.SingletonConsumerSet.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._reraise_augmented()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.SimpleInvoker.invoke_process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common._OutputProcessor.process_outputs()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.SingletonConsumerSet.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/worker/operations.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.worker.operations.DoOperation.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.receive()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner._reraise_augmented()


    /opt/conda/lib/python3.7/site-packages/future/utils/__init__.py in raise_with_traceback(exc, traceback)
        444         if traceback == Ellipsis:
        445             _, _, traceback = sys.exc_info()
    --> 446         raise exc.with_traceback(traceback)
        447 
        448 else:


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.DoFnRunner.process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/runners/common.cpython-37m-x86_64-linux-gnu.so in apache_beam.runners.common.SimpleInvoker.invoke_process()


    /opt/conda/lib/python3.7/site-packages/apache_beam/transforms/core.py in <lambda>(x)
       1435     wrapper = lambda x, *args, **kwargs: [fn(x, *args, **kwargs)]
       1436   else:
    -> 1437     wrapper = lambda x: [fn(x)]
       1438 
       1439   label = 'Map(%s)' % ptransform.label_from_callable(fn)


    <ipython-input-39-d51443206d2e> in format_output(element)
         21 def format_output(element):
         22     name, count = element
    ---> 23     return ', '.join((name.encode('ascii'),str(count),'Regular employee'))
         24 
         25 p = beam.Pipeline()


    TypeError: sequence item 0: expected str instance, bytes found [while running 'Cell 39: composite accoubts/Regular accounts employee']



```python
# Sample the first 20 results, remember there are no ordering guarantees.
!{('head -n 20 data/Account-00000-of-00001')}
!{('head -n 20 data/HR-00000-of-00001')}
```


```python

```


    ---------------------------------------------------------------------------

    OSError                                   Traceback (most recent call last)

    <ipython-input-38-31e83525d537> in <module>
         32                       p
         33                       | "Read from text file" >> beam.io.ReadFromText('dept_data.txt')
    ---> 34                       | "Split rows" >> beam.Map(SplitRow)
         35                    )
         36 


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/textio.py in __init__(self, file_pattern, min_bundle_size, compression_type, strip_trailing_newlines, coder, validate, skip_header_lines, **kwargs)
        540         file_pattern, min_bundle_size, compression_type,
        541         strip_trailing_newlines, coder, validate=validate,
    --> 542         skip_header_lines=skip_header_lines)
        543 
        544   def expand(self, pvalue):


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/textio.py in __init__(self, file_pattern, min_bundle_size, compression_type, strip_trailing_newlines, coder, buffer_size, validate, skip_header_lines, header_processor_fns)
        124     super(_TextSource, self).__init__(file_pattern, min_bundle_size,
        125                                       compression_type=compression_type,
    --> 126                                       validate=validate)
        127 
        128     self._strip_trailing_newlines = strip_trailing_newlines


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/filebasedsource.py in __init__(self, file_pattern, min_bundle_size, compression_type, splittable, validate)
        123     self._splittable = splittable
        124     if validate and file_pattern.is_accessible():
    --> 125       self._validate()
        126 
        127   def display_data(self):


    /opt/conda/lib/python3.7/site-packages/apache_beam/options/value_provider.py in _f(self, *args, **kwargs)
        138         if not obj.is_accessible():
        139           raise error.RuntimeValueProviderError('%s not accessible' % obj)
    --> 140       return fnc(self, *args, **kwargs)
        141     return _f
        142   return _check_accessible


    /opt/conda/lib/python3.7/site-packages/apache_beam/io/filebasedsource.py in _validate(self)
        184     if len(match_result.metadata_list) <= 0:
        185       raise IOError(
    --> 186           'No files found based on the file pattern %s' % pattern)
        187 
        188   def split(


    OSError: No files found based on the file pattern dept_data.txt



```python

```
