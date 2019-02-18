# Tangle Streaming examples in Flink

**_Note:_** This project is based on the [flink-tangle-source](https://github.com/Citrullin/flink-tangle-source) library.
This is a proof of concept and should not be used in production environments. 
Feel free to contribute, so that it eventually becomes production ready.

This project has a few tangle streaming examples in [Flink](https://flink.apache.org/). 

## Use the examples

### 1. Install java & sbt

You need to install java >= 8 & [sbt](https://www.scala-sbt.org/), before you can execute the examples.

### 2. Build & publish dependencies

Since this library is not production ready, you need to build & publish the dependencies locally. 
You can find the description about the process in the repositories:
- [Tangle Stream provider repository](https://github.com/Citrullin/tangle-streaming)
- [Flink Tangle source repository](https://github.com/Citrullin/flink-tangle-source)


### 3. Run the application

If the dependencies are available, you can run the example in the following way:

```bash
sbt run
```

SBT will ask you which application you want to run. Just select the application you want to run.


## Troubleshooting

### The ZeroMQ server is not available

[TangleBeat](http://tanglebeat.com/page/internals) has a list of public tangle ZMQ server. 
Just pick one and change the application.conf in src/main/resources