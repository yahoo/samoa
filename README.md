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

[![Build Status](https://travis-ci.org/yahoo/samoa.svg?branch=master)](https://travis-ci.org/yahoo/samoa)

SAMOA: Scalable Advanced<br/>Massive Online Analysis.
=================

### This repository is discontinued. The development of SAMOA has moved over to the [Apache Software Foundation](http://samoa.incubator.apache.org). Please subscribe to the [dev mailing list](http://incubator.apache.org/projects/samoa.html) to participate in the development, and use [Jira](https://issues.apache.org/jira/browse/SAMOA) to report bugs and propose new features. The new repository is mirrored on [GitHub](https://github.com/apache/incubator-samoa).

SAMOA is a platform for mining on big data streams.
It is a distributed streaming machine learning (ML) framework that contains a 
programing abstraction for distributed streaming ML algorithms.

SAMOA enables development of new ML algorithms without dealing with 
the complexity of underlying streaming processing engines (SPE, such 
as Apache Storm and Apache S4). SAMOA also provides extensibility in integrating
new SPEs into the framework. These features allow SAMOA users to develop 
distributed streaming ML algorithms once and to execute the algorithms 
in multiple SPEs, i.e., code the algorithms once and execute them in multiple SPEs.

## Build

###Storm mode

Simply clone the repository and install SAMOA.
```bash
git clone git@github.com:yahoo/samoa.git
cd samoa
mvn -Pstorm package
```

The deployable jar for SAMOA will be in `target/SAMOA-Storm-0.3.0-SNAPSHOT.jar`.

###S4 mode

If you want to compile SAMOA for S4, you will need to install the S4 dependencies
manually as explained in [Executing SAMOA with Apache S4](../../wiki/Executing-SAMOA-with-Apache-S4).

Once the dependencies if needed are installed, you can simply clone the repository and install SAMOA.

```bash
git clone git@github.com:yahoo/samoa.git
cd samoa
mvn -Ps4 package
```

###Local mode

If you want to test SAMOA in a local environment, simply clone the repository and install SAMOA.

```bash
git clone git@github.com:yahoo/samoa.git
cd samoa
mvn package
```

The deployable jar for SAMOA will be in `target/SAMOA-Local-0.3.0-SNAPSHOT.jar`.

## Slides

[SAMOA Slides](https://speakerdeck.com/gdfm/samoa-a-platform-for-mining-big-data-streams)

G. De Francisci Morales [SAMOA: A Platform for Mining Big Data Streams](http://melmeric.files.wordpress.com/2013/04/samoa-a-platform-for-mining-big-data-streams.pdf)
Keynote Talk at [RAMSS ’13](http://www.ramss.ws/2013/program/): 2nd International Workshop on Real-Time Analysis and Mining of Social Streams WWW, Rio De Janeiro, 2013.

## License

The use and distribution terms for this software are covered by the
Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0.html).

