package com.yahoo.labs.samoa.topology.impl;

/*
 * #%L
 * SAMOA
 * %%
 * Copyright (C) 2013 - 2014 Yahoo! Inc.
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

import java.util.List;
import java.util.Set;

import org.apache.samza.config.MapConfig;
import org.apache.samza.job.JobRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.topology.Topology;
import com.yahoo.labs.samoa.topology.impl.SamzaStream.SamzaSystemStream;
import com.yahoo.labs.samoa.utils.SamzaConfigFactory;
import com.yahoo.labs.samoa.utils.SystemsUtils;

/**
 * This class will submit a list of Samza jobs with 
 * the Configs generated from the input topology
 * 
 * @author Anh Thu Vu
 *
 */
public class SamzaEngine {
	
	private static final Logger logger = LoggerFactory.getLogger(SamzaEngine.class);
	
	/*
	 * Singleton instance
	 */
	private static SamzaEngine engine = new SamzaEngine();
	
	private String zookeeper;
	private String kafka;
	private int kafkaBatchSize;
	private String kafkaProducerType;
	private boolean isLocalMode;
	private String yarnPackagePath;
	private String yarnConfHome;
	
	private String kryoRegisterFile;
	
	private int amMem;
	private int containerMem;
	private int piPerContainerRatio;
	
	private void _submitTopology(Topology topo) {
		SamzaTopology topology = (SamzaTopology) topo;
		
		// Setup SamzaConfigFactory
		SamzaConfigFactory configFactory = new SamzaConfigFactory();
		configFactory.setLocalMode(isLocalMode)
		.setZookeeper(zookeeper)
		.setKafka(kafka, kafkaProducerType, kafkaBatchSize)
		.setYarnPackage(yarnPackagePath)
		.setAMMemory(amMem)
		.setContainerMemory(containerMem)
		.setPiPerContainerRatio(piPerContainerRatio)
		.setKryoRegisterFile(kryoRegisterFile);
		
		// Generate the list of Configs
		List<MapConfig> configs;
		try {
			// ConfigFactory generate a list of configs
			// Serialize a map of PIs and store in a file in the jar at jarFilePath
			// (in dat/ folder)
			configs = configFactory.getMapConfigsForTopology(topology);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		// Submit the jobs with those configs
		for (MapConfig config:configs) {
			logger.info("Config:{}",config);
			JobRunner jobRunner = new JobRunner(config);
			jobRunner.run();
		}
	}

	private void _setupSystemsUtils() {
		// Setup Utils
		if (!isLocalMode)
			SystemsUtils.setHadoopConfigHome(yarnConfHome);
		SystemsUtils.setZookeeper(zookeeper);
	}
	
	/*
	 * Setter methods
	 */
	public static SamzaEngine getEngine() {
		return engine;
	}
	
	public SamzaEngine setZooKeeper(String zk) {
		this.zookeeper = zk;
		return this;
	}
	
	public SamzaEngine setKafka(String kafka) {
		this.kafka = kafka;
		return this;
	}
	
	public SamzaEngine setKafkaBatchSize(int kafkaBatchSize) {
		this.kafkaBatchSize = kafkaBatchSize;
		return this;
	}
	
	public SamzaEngine setLocalMode(boolean isLocal) {
		this.isLocalMode = isLocal;
		return this;
	}
	
	public SamzaEngine setYarnPackage(String yarnPackagePath) {
		this.yarnPackagePath = yarnPackagePath;
		return this;
	}
	
	public SamzaEngine setConfigHome(String configHome) {
		this.yarnConfHome = configHome;
		return this;
	}
	
	public SamzaEngine setAMMemory(int mem) {
		this.amMem = mem;
		return this;
	}
	
	public SamzaEngine setContainerMemory(int mem) {
		this.containerMem = mem;
		return this;
	}
	
	public SamzaEngine setPiPerContainerRatio(int piPerContainer) {
		this.piPerContainerRatio = piPerContainer;
		return this;
	}
	
	public SamzaEngine setKafkaProducerType(String type) {
		this.kafkaProducerType = type;
		return this;
	}
	
	public SamzaEngine setKryoRegisterFile(String registerFile) {
		this.kryoRegisterFile = registerFile;
		return this;
	}
	
	/**
	 * Submit a list of Samza jobs correspond to the submitted 
	 * topology
	 * 
	 * @param topo
	 *            the submitted topology
	 */
	public static void submitTopology(Topology topo) {
		// Setup SystemsUtils
		engine._setupSystemsUtils();
		
		// Create kafka streams
		Set<Stream> streams = topo.getStreams(); 
		for (Stream stream:streams) {
			SamzaStream samzaStream = (SamzaStream) stream;
			List<SamzaSystemStream> systemStreams = samzaStream.getSystemStreams();
			for (SamzaSystemStream systemStream:systemStreams) {
				// all streams should be kafka streams
				SystemsUtils.createKafkaTopic(systemStream.getStream(),systemStream.getParallelism());
			}
		}
		
		// Submit topology
		engine._submitTopology(topo);
	}
}
