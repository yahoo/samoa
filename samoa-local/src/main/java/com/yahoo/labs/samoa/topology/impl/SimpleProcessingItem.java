/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.labs.samoa.topology.impl;

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

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.Processor;
import com.yahoo.labs.samoa.topology.IProcessingItem;
import com.yahoo.labs.samoa.topology.ProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.utils.PartitioningScheme;
import com.yahoo.labs.samoa.utils.StreamDestination;

/**
 *
 * @author abifet
 */
class SimpleProcessingItem implements ProcessingItem {
	
    protected Processor processor;
    private int processingItemParalellism;
    private IProcessingItem[] arrayProcessingItem;

    public IProcessingItem getProcessingItem(int i) {
        return arrayProcessingItem[i];
    }

    SimpleProcessingItem(Processor processor, int paralellism) {
        this.processor = processor;
        this.processingItemParalellism = paralellism;
    }
    
    private ProcessingItem addInputStream(Stream inputStream, PartitioningScheme scheme) {
		StreamDestination destination = new StreamDestination(this, this.processingItemParalellism, scheme);
		((SimpleStream)inputStream).addDestination(destination);
		return this;
	}

    public ProcessingItem connectInputShuffleStream(Stream inputStream) {
    	this.addInputStream(inputStream, PartitioningScheme.SHUFFLE);
        return this;
    }

    public ProcessingItem connectInputKeyStream(Stream inputStream) {
    	this.addInputStream(inputStream, PartitioningScheme.GROUP_BY_KEY);
        return this;
    }

    public ProcessingItem connectInputAllStream(Stream inputStream) {
    	this.addInputStream(inputStream, PartitioningScheme.BROADCAST);
        return this;
    }

    public int getParalellism() {
        return processingItemParalellism;
    }

    public Processor getProcessor() {
        return this.processor;
    }

    public SimpleProcessingItem copy() {
        SimpleProcessingItem ret = new SimpleProcessingItem(this.processor.newProcessor(this.processor), 0); // this.getParalellism());
        return ret;
    }

    public void processEvent(ContentEvent event, int counter) {
        int paralellism = this.getParalellism();
        if (this.arrayProcessingItem == null && paralellism > 0) {
            //Init processing elements, the first time they are needed
            this.arrayProcessingItem = new IProcessingItem[paralellism];
            for (int j = 0; j < paralellism; j++) {
                arrayProcessingItem[j] = this.copy();
                arrayProcessingItem[j].getProcessor().onCreate(j);
                //System.out.println(j + " PROCESSOR create " + arrayProcessingItem[j].getProcessor());
            }
        }
        if (this.arrayProcessingItem != null) {
            this.getProcessingItem(counter).getProcessor().process(event);
        }
    }
}
