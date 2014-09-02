SAMOA: Scalable Advanced Massive Online Analysis.
=================
SAMOA is a platform for mining on big data streams.
It is a distributed streaming machine learning (ML) framework that contains a 
programing abstraction for distributed streaming ML algorithms.

SAMOA enables development of new ML algorithms without dealing with 
the complexity of underlying streaming processing engines (SPE, such 
as Apache Storm and Apache S4). SAMOA also provides extensibility in integrating
new SPEs into the framework. These features allow SAMOA users to develop 
distributed streaming ML algorithms once and to execute the algorithms 
in multiple SPEs, i.e., code the algorithms once and execute them in multiple SPEs.

# Setup Sentinel
This is a fork of ```SAMOA``` for [Sentinel](https://github.com/ambodi/sentinel) project.
  
## Install
Clone this repository
```sh
git clone https://github.com/ambodi/sentinel
```
Put Sentinel under 
```
samoa-api/src/main/java/com/yahoo/labs/samoa/sentinel
```

Add ```twitter4j.properties``` file in the root of the project. More info at [Twitter 4J's Documentation on Generic properties](http://twitter4j.org/en/configuration.html "Title")

## Build
* 
```
mvn clean install
```

* 
```
mvn package 
```
(Local Cluster)
* 
```
mvn -Pstorm package
```
(Apache Storm Cluster)

## Tasks

### Real-time Sentiment Analysis on Twitter Public Stream 
#### Bash
Using Vertical Hoeffding Tree as a distributed parallel classification algorithm, you can perform sentiment analysis on [Twitter Public Stream](https://dev.twitter.com/docs/streaming-apis/streams/public) with Prequential Evaluation Task. 

To perform sentiment analysis on a sample of 100000 tweets in real-time with 4 parallel nodes in your local cluster, run

```
bin/samoa local target/SAMOA-Local-0.2.0-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -f 100000 -l (classifiers.trees.VerticalHoeffdingTree -p 4) -s com.yahoo.labs.samoa.sentinel.model.TwitterStreamInstance"
```

Or if you run it in Apache Storm, run

```
bin/samoa storm target/SAMOA-Storm-0.2.0-SNAPSHOT.jar "PrequentialEvaluation -d /tmp/dump.csv -i 1000000 -f 100000 -l (classifiers.trees.VerticalHoeffdingTree -p 4) -s com.yahoo.labs.samoa.sentinel.model.TwitterStreamInstance"
```
### Code
Put the following code under ```samoa-local(samoa-storm)/src/main/java/com/yahoo/labs/samoa/```:

    public static void main( String[] args ) {
        PrequentialEvaluation pe = new PrequentialEvaluation();
        pe.setFactory(new SimpleComponentFactory());

        pe.dumpFileOption.setValueViaCLIString("/tmp/dump.csv");
        pe.instanceLimitOption.setValue(50);
        pe.sampleFrequencyOption.setValue(5);
        pe.learnerOption.setValueViaCLIString("classifiers.trees.VerticalHoeffdingTree -p 1");
        pe.streamTrainOption.setValueViaCLIString(TwitterStreamInstance.class.getName());

        pe.init();
    }

Run ```mvn -X exec:java -Dexec.mainClass=com.yahoo.labs.samoa.app```


This is preferred if you are developing and want to make use of debug mode.  
<!--
  Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->


## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).

