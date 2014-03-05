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
import com.yahoo.labs.samoa.core.EntranceProcessor;
import com.yahoo.labs.samoa.topology.EntranceProcessingItem;
import com.yahoo.labs.samoa.topology.Stream;

class SimpleEntranceProcessingItem implements EntranceProcessingItem {

    protected EntranceProcessor entranceProcessor;
    protected Stream outputStream;

    public SimpleEntranceProcessingItem(EntranceProcessor processor) {
        this.entranceProcessor = processor;
    }

    @Override
    public EntranceProcessor getProcessor() {
        return this.entranceProcessor;
    }

    public boolean injectNextEvent() {
        if (entranceProcessor.hasNext()) {
            ContentEvent nextEvent = this.entranceProcessor.nextEvent();
            outputStream.put(nextEvent);
            return true;
        } else
            return false;
        // return !nextEvent.isLastEvent();
    }
    
    public void startSendingEvents() {
		if (outputStream == null) 
			throw new IllegalStateException("Try sending events from EntrancePI while outputStream is not set.");
		
		while(!entranceProcessor.isFinished()) {
			if (!injectNextEvent()) {	
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					break;
				}
			}
		}
		
		// Send last event
		this.outputStream.put(entranceProcessor.nextEvent());
	}

    @Override
    public EntranceProcessingItem setOutputStream(Stream stream) {
        if (this.outputStream != null) {
        	if (this.outputStream == stream) return this; // Allow reassign to the same output stream
        	else
        		throw new IllegalStateException("Output stream for an EntrancePI sohuld be initialized only once");
        }
        this.outputStream = stream;
        return this;
    }
}