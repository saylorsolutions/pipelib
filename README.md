# Pipelib

Pipelib is a set of utilities that makes it easy to build pipeline processes using goroutines.
There are a few different aspects.

[pipeline.go](pipeline.go) contains producer, handler, and consumer functions as the elements of a pipeline.
Pipelines are channel based, but consumers of the lib can write code in a synchronous style using the different handlers.
- `Produce` is used to establish a producer that adds elements to the pipeline.
- `Handle` is used to establish an intermediary step within the pipeline that processes input and returns output.
- `Consume` is used to establish a terminal operation within a pipeline.
- `RunPipeline` is used to execute the pipeline functions which will coordinate goroutines and channels to support your pipeline.
- There is also middleware, filtering, and batching support.

[context.go](context.go) contains the custom `context.Context` implementation that is made available to `PipelineFunc`s.
It provides error reporting, logging, and all of the normal context operation we would expect.

[future.go](future.go) provides a means to wait for asynchronous processing and return a result with an error.
This is helpful when coordinating with remote resources that may or may not respond in a timely manner.

[counter.go](counter.go) provides an interval counter that can be used to understand the performance characteristics of a pipeline.
Middleware functions are provided in [pipeline.go](pipeline.go) that interact with counters when a handler is called.
- Uses a fixed ring buffer for fixed memory usage and minimal allocations, even for large buffers.
- Can return the average count over all recorded intervals to understand throughput.
- Can return the total count over all recorded intervals to understand the processing volume.
- Can be used with arbitrary intervals, and there is a function to calculate the buffer size needed to cover the length of time required.
- Can also return all recorded intervals for further processing or export.

[channels.go](channels.go) provides some utilities that can be helpful in more complicated or branching pipeline scenarios.
