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

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.topology.IProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;
import com.yahoo.labs.samoa.utils.StreamDestination;

/**
 * 
 * @author abifet
 */
class SimpleStream implements Stream {
    private List<StreamDestination> destinations;
    private int processingItemParalellism;
    private int shuffleCounter;
    private int maxCounter;

    public int getParalellism() {
        return processingItemParalellism;
    }

    public void setParalellism(int paralellism) {
        this.processingItemParalellism = paralellism;
    }

    SimpleStream(IProcessingItem sourcePi) {
    	this.destinations = new LinkedList<StreamDestination>();
    	this.shuffleCounter = 0;
    	this.maxCounter = 1;
    }

    public void put(ContentEvent event) {
        this.shuffleCounter++;
        if (this.shuffleCounter >= this.maxCounter) this.shuffleCounter = 0;
        
        SimpleProcessingItem pi;
        for (StreamDestination destination:destinations) {
            pi = (SimpleProcessingItem) destination.getProcessingItem();
            switch (destination.getPartitioningScheme()) {
            case SHUFFLE:
                pi.processEvent(event, shuffleCounter);
                break;
            case GROUP_BY_KEY:
                HashCodeBuilder hb = new HashCodeBuilder();
                hb.append(event.getKey());
                int key = hb.build() % getParalellism();
                pi.processEvent(event, key);
                break;
            case BROADCAST:
                for (int p = 0; p < this.getParalellism(); p++) {
                    pi.processEvent(event, p);
                }
                break;
            }
        }
    }

    public String getStreamId() {
        return null;
    }

    public void addDestination(StreamDestination destination) {
        this.destinations.add(destination);
    }
}
