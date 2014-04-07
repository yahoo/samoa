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
	public void setProcessor(EntranceProcessor p) {
		this.processor = p;
	}
	public EntranceProcessor getProcessor() {
		return this.processor;
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
	 * Output Stream
	 */
	public EntranceProcessingItem setOutputStream(Stream outputStream) {
		if (this.outputStream != null && this.outputStream != outputStream) {
			throw new IllegalStateException("Cannot overwrite output stream of EntranceProcessingItem");
		} else 
			this.outputStream = outputStream;
		return this;
	}
	
	public Stream getOutputStream() {
		return this.outputStream;
	}
	
	/*
	 * Inject event
	 */
	public boolean injectNextEvent() {
		if (processor.hasNext()) {
			ContentEvent event = processor.nextEvent();
			outputStream.put(event);
			return true;
		}
		return false;
	}
	
	public void start() {
		while (!processor.isFinished()) 
			if (!this.injectNextEvent())
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
		// Inject the last event
		ContentEvent event = processor.nextEvent();
		outputStream.put(event);
	}
}
