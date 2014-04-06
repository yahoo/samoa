package com.yahoo.labs.samoa.topology;

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

import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.utils.PartitioningScheme;

/**
 * @author Anh Thu Vu
 *
 */
public abstract class AbstractProcessingItem implements ProcessingItem {
	private String name;
	private int parallelism;
	private Processor processor;
	
	/*
	 * Constructor
	 */
	public AbstractProcessingItem() {
		this(null);
	}
	public AbstractProcessingItem(Processor processor) {
		this.processor = processor;
		this.parallelism = 1; // default to 1
	}
	
	/*
	 * Create & destroy
	 */
	public void onCreate(int id) {
		processor.onCreate(id);
	}
	
	public void onDestroy() {
		// do nothing
	}
	
	/*
	 * Processor
	 */
	public void setProcessor(Processor processor) {
		this.processor = processor;
	}
	
	public Processor getProcessor() {
		return this.processor;
	}
	
	/*
	 * Parallelism 
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	@Override
	public int getParallelism() {
		return this.parallelism;
	}
	
	/*
	 * Name/ID
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	public String getName() {
		return this.name;
	}
	
	/*
	 * Add input streams
	 */
	protected abstract ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme);

    public ProcessingItem connectInputShuffleStream(Stream inputStream) {
    	return this.addInputStream(inputStream, PartitioningScheme.SHUFFLE);
    }

    public ProcessingItem connectInputKeyStream(Stream inputStream) {
    	return this.addInputStream(inputStream, PartitioningScheme.GROUP_BY_KEY);
    }

    public ProcessingItem connectInputAllStream(Stream inputStream) {
    	return this.addInputStream(inputStream, PartitioningScheme.BROADCAST);
    }
}
