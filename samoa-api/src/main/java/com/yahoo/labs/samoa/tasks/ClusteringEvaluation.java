package com.yahoo.labs.samoa.tasks;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 Yahoo! Inc.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.github.javacliparser.ClassOption;
import com.github.javacliparser.Configurable;
import com.github.javacliparser.FileOption;
import com.github.javacliparser.FloatOption;
import com.github.javacliparser.IntOption;
import com.github.javacliparser.StringOption;
import com.yahoo.labs.samoa.core.TopologyStarter;
import com.yahoo.labs.samoa.learners.Learner;
import com.yahoo.labs.samoa.evaluation.ClusteringEvaluatorProcessor;
import com.yahoo.labs.samoa.streams.ClusteringSourceProcessor;
import com.yahoo.labs.samoa.streams.ClusteringSourceTopologyStarter;
import com.yahoo.labs.samoa.learners.clusterers.simple.DistributedClusterer;
import com.yahoo.labs.samoa.moa.streams.InstanceStream;
import com.yahoo.labs.samoa.moa.streams.clustering.ClusteringStream;
import com.yahoo.labs.samoa.moa.streams.clustering.RandomRBFGeneratorEvents;
import com.yahoo.labs.samoa.topology.ComponentFactory;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.TopologyBuilder;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clustering Evaluation task is a scheme in evaluating performance of clusterers
 * 
 * @author Arinto Murdopo, Antonio Severien
 * 
 */
public class ClusteringEvaluation implements Task, Configurable {

    private static final long serialVersionUID = -8246537378371580550L;

    private static Logger logger = LoggerFactory.getLogger(ClusteringEvaluation.class);

    public ClassOption learnerOption = new ClassOption("learner", 'l', "Clusterer to train.", Learner.class,
    // SingleLearner.class.getName()
            DistributedClusterer.class.getName());

    public ClassOption streamTrainOption = new ClassOption("streamTrain", 's', "Stream to learn from.", InstanceStream.class,
            RandomRBFGeneratorEvents.class.getName());

    public IntOption instanceLimitOption = new IntOption("instanceLimit", 'i', "Maximum number of instances to test/train on  (-1 = no limit).", 100000, -1,
            Integer.MAX_VALUE);

    public IntOption measureCollectionTypeOption = new IntOption("measureCollectionType", 'm', "Type of measure collection", 0, 0, Integer.MAX_VALUE);

    public IntOption timeLimitOption = new IntOption("timeLimit", 't', "Maximum number of seconds to test/train for (-1 = no limit).", -1, -1,
            Integer.MAX_VALUE);

    public IntOption sampleFrequencyOption = new IntOption("sampleFrequency", 'f', "How many instances between samples of the learning performance.", 1000, 0,
            Integer.MAX_VALUE);

    public StringOption evaluationNameOption = new StringOption("evalutionName", 'n', "Identifier of the evaluation", "Clustering__"
            + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));

    public FileOption dumpFileOption = new FileOption("dumpFile", 'd', "File to append intermediate csv results to", null, "csv", true);

    public FloatOption samplingThresholdOption = new FloatOption("samplingThreshold", 'a', "Ratio of instances sampled that will be used for evaluation.", 0.5,
            0.0, 1.0);
    
    // Default=0: no delay/waiting
    public IntOption sourceDelayOption = new IntOption("sourceDelay", 'w', "How many miliseconds between injections of two instances.", 0, 0, Integer.MAX_VALUE);
    
    private ClusteringSourceProcessor source;

    private ClusteringSourceTopologyStarter starter;

    // private EntranceProcessingItem sourcePi;

    private Stream sourcePiOutputStream;

    private Stream sourcePiEvalStream;

    private Learner learner;

    private ClusteringEvaluatorProcessor evaluator;

    // private ProcessingItem evaluatorPi;

    private Stream evaluatorPiInputStream;

    private Topology topology;

    private TopologyBuilder builder;

    public void getDescription(StringBuilder sb, int indent) {
        sb.append("Clustering evaluation");
    }

    @Override
    public void init() {
        // TODO remove the if statement
        // theoretically, dynamic binding will work here!
        // test later!
        // for now, the if statement is used by Storm

        if (builder == null) {
            builder = new TopologyBuilder();
            logger.debug("Sucessfully instantiating TopologyBuilder");

            builder.initTopology(evaluationNameOption.getValue(), sourceDelayOption.getValue());
            logger.debug("Sucessfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());
        }

        // instantiate PrequentialSourceProcessor and its output stream (sourcePiOutputStream)
        source = new ClusteringSourceProcessor();
        InstanceStream streamTrain = (InstanceStream) this.streamTrainOption.getValue();
        source.setStreamSource(streamTrain);

        // TODO: refactor component creation, use Factory.createTopoStarter(this)
        // inside Factory, we will use the public options attribute
        // TODO: integrate time limit into TopologyStarter and PrequentialSourceProcessor

        // starter = new ClusteringSourceTopologyStarter(source, instanceLimitOption.getValue(), this.samplingThresholdOption.getValue());
        // sourcePi = builder.createEntrancePi(source, starter);
        // sourcePiOutputStream = builder.createStream(sourcePi);
        builder.addEntranceProcessor(source); // FIXME put the starter code inside the platform code
        sourcePiOutputStream = builder.createStream(source);
        // starter.setInputStream(sourcePiOutputStream); // FIXME set stream in the EntrancePI
        logger.debug("Sucessfully instantiating ClusteringSourceProcessor");

        sourcePiEvalStream = builder.createStream(source);
        starter.setEvalStream(sourcePiEvalStream);

        // instantiate learner and connect it to sourcePiOutputStream
        learner = (Learner) this.learnerOption.getValue();
        learner.init(builder, source.getDataset(), 1);
        this.builder.connectInputShuffleStream(sourcePiOutputStream, learner.getInputProcessor());
        logger.debug("Sucessfully instantiating Learner");

        evaluatorPiInputStream = learner.getResultStream();
        evaluator = new ClusteringEvaluatorProcessor.Builder(// (ClassificationPerformanceEvaluator) this.evaluatorOption.getValue())
                // .samplingFrequency(
                sampleFrequencyOption.getValue()).dumpFile(dumpFileOption.getFile()).decayHorizon(((ClusteringStream) streamTrain).getDecayHorizon()).build();

        // evaluatorPi = builder.createPi(evaluator);
        // evaluatorPi.connectInputShuffleStream(evaluatorPiInputStream);
        // evaluatorPi.connectInputAllStream(sourcePiEvalStream);
        builder.addProcessor(evaluator);
        builder.connectInputShuffleStream(evaluatorPiInputStream, evaluator);
        builder.connectInputAllStream(sourcePiEvalStream, evaluator);
        logger.debug("Sucessfully instantiating EvaluatorProcessor");

        topology = builder.build();
        logger.debug("Sucessfully building the topology");
    }

    @Override
    public void setFactory(ComponentFactory factory) {
        // TODO unify this code with init()
        // for now, it's used by S4 App
        // dynamic binding theoretically will solve this problem
        builder = new TopologyBuilder(factory);
        logger.debug("Sucessfully instantiating TopologyBuilder");

        builder.initTopology(evaluationNameOption.getValue(), sourceDelayOption.getValue());
        logger.debug("Sucessfully initializing SAMOA topology with name {}", evaluationNameOption.getValue());

    }

    public Topology getTopology() {
        return topology;
    }

    // @Override
    // public TopologyStarter getTopologyStarter() {
    // return this.starter;
    //
    // }
}
