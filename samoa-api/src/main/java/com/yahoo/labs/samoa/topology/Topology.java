package com.yahoo.labs.samoa.topology;

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

import java.util.HashSet;
import java.util.Set;

/**
 * Topology abstract class.
 * 
 */
public abstract class Topology {

	private String topoName;
    protected Set<Stream> streams;
    protected Set<IProcessingItem> processingItems;
    protected Set<EntranceProcessingItem> entranceProcessingItems;
    private String task; // TODO: check if task is needed here

    protected Topology(String name) {
    	this.topoName = name;
    	this.streams = new HashSet<Stream>();
        this.processingItems = new HashSet<IProcessingItem>();
        this.entranceProcessingItems = new HashSet<EntranceProcessingItem>();
    }
    
    /**
     * Gets the name of this topology
     * 
     * @return name of the topology
     */
    public String getTopologyName() {
    	return this.topoName;
    }
    
    /**
     * Adds an Entrance processing item to the topology.
     * 
     * @param epi
     * 			Entrance processing item
     */
    protected void addEntranceProcessingItem(EntranceProcessingItem epi) {
    	this.entranceProcessingItems.add(epi);
    	this.addProcessingItem(epi);
    }
    
    /**
     * Gets entrance processing items in the topology
     * 
     * @return the set of processing items
     */
    public Set<EntranceProcessingItem> getEntranceProcessingItems() {
    	return this.entranceProcessingItems;
    }

    /**
     * Add processing item to topology.
     * 
     * @param procItem
     *            Processing item.
     */
    protected void addProcessingItem(IProcessingItem procItem) {
        addProcessingItem(procItem, 1);
    }

    /**
     * Add processing item to topology.
     * 
     * @param procItem
     *            Processing item.
     * @param parallelismHint
     *            Processing item parallelism level.
     */
    protected void addProcessingItem(IProcessingItem procItem, int parallelismHint) {
        this.processingItems.add(procItem);
    }
    
    /**
     * Gets processing items in the topology (including entrance processing items)
     * 
     * @return the set of processing items
     */
    public Set<IProcessingItem> getProcessingItems() {
    	return this.processingItems;
    }

    /**
     * Add stream to topology.
     * 
     * @param stream
     */
    protected void addStream(Stream stream) {
        this.streams.add(stream);
    }
    
    /**
     * Gets streams in the topology
     * 
     * @return the set of streams
     */
    public Set<Stream> getStreams() {
    	return this.streams;
    }

    /**
     * Sets evaluation task.
     * 
     * @param task
     */
    public void setEvaluationTask(String task) {
        this.task = task;
    }

    /**
     * Gets evaluation task.
     * 
     * @return
     */
    public String getEvaluationTask() {
        return task;
    }  
}
