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

public abstract class AbstractStream implements Stream {
	private String name;
	private IProcessingItem sourcePi;
 
	/*
	 * Constructor
	 */
	public AbstractStream() {
		this(null);
	}
	public AbstractStream(IProcessingItem sourcePi) {
		this.sourcePi = sourcePi;
	}
	
	public IProcessingItem getSourceProcessingItem() {
		return this.sourcePi;
	}

    /*
     * Process event
     */
    @Override
    public abstract void put(ContentEvent event);

    /*
     * Stream name
     */
    @Override
    public abstract String getStreamId();
    
    public void setName(String name) {
    	this.name = name;
    }
    
    public String getName() {
    	return this.name;
    }

}
