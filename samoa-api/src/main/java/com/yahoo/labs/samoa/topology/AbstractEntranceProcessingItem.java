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

import com.yahoo.labs.samoa.core.ContentEvent;
import com.yahoo.labs.samoa.core.EntranceProcessor;

/**
 * Helper class for EntranceProcessingItem implementation.
 * @author Anh Thu Vu
 *
 */
public abstract class AbstractEntranceProcessingItem implements EntranceProcessingItem {
	private EntranceProcessor processor;
	private String name;
	private Stream outputStream;
	
	/*
	 * Constructor
	 */
	public AbstractEntranceProcessingItem() {
		this(null);
	}
	public AbstractEntranceProcessingItem(EntranceProcessor processor) {
		this.processor = processor;
	}
	
	/*
	 * Processor
	 */
	/**
	 * Set the entrance processor for this EntranceProcessingItem
	 * @param processor
	 * 			the processor
	 */
	protected void setProcessor(EntranceProcessor processor) {
		this.processor = processor;
	}
	
	/**
	 * Get the EntranceProcessor of this EntranceProcessingItem.
	 * @return the EntranceProcessor
	 */
	public EntranceProcessor getProcessor() {
		return this.processor;
	}
	
	/*
	 * Name/ID
	 */
	/**
	 * Set the name (or ID) of this EntranceProcessingItem
	 * @param name
	 */
	public void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Get the name (or ID) of this EntranceProcessingItem
	 * @return the name (or ID)
	 */
	public String getName() {
		return this.name;
	}
	
	/*
	 * Output Stream
	 */
	/**
	 * Set the output stream of this EntranceProcessingItem.
	 * An EntranceProcessingItem should have only 1 single output stream and
	 * should not be re-assigned.
	 * @return this EntranceProcessingItem
	 */
	public EntranceProcessingItem setOutputStream(Stream outputStream) {
		if (this.outputStream != null && this.outputStream != outputStream) {
			throw new IllegalStateException("Cannot overwrite output stream of EntranceProcessingItem");
		} else 
			this.outputStream = outputStream;
		return this;
	}
	
	/**
	 * Get the output stream of this EntranceProcessingItem.
	 * @return the output stream
	 */
	public Stream getOutputStream() {
		return this.outputStream;
	}
	
	/*
	 * Inject event
	 */
	/**
	 * If there are available events, first event in the queue will be
	 * sent out on the output stream. 
	 * @return true if there is (at least) one available event and it was sent out
	 *         false otherwise 
	 */
	public boolean injectNextEvent() {
		if (processor.hasNext()) {
			ContentEvent event = processor.nextEvent();
			outputStream.put(event);
			return true;
		}
		return false;
	}
	
	/**
	 * Start sending events by calling {@link #injectNextEvent()}. If there are no available events, 
	 * it will wait by calling {@link #waitForNewEvents()} before attempting to send again.
	 */
	public void start() {
		while (!processor.isFinished()) 
			if (!this.injectNextEvent()) {
				try {
					waitForNewEvents();
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}
		// Inject the last event
		ContentEvent event = processor.nextEvent();
		outputStream.put(event);
	}
	
	/**
	 * Method to wait for an amount of time when there are no available events.
	 * Implementation of EntranceProcessingItem should override this method to 
	 * implement non-blocking wait or to adjust the amount of time.
	 */
	protected void waitForNewEvents() throws Exception {
		Thread.sleep(100);
	}
}
