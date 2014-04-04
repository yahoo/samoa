package com.yahoo.labs.samoa.topology;

import com.yahoo.labs.samoa.core.EntranceProcessor;

/**
 * @author Anh Thu Vu
 *
 */
public abstract class AbstractEntranceProcessingItem implements EntranceProcessingItem {
	protected int parallelism;
	protected EntranceProcessor processor;
	protected String name;
	protected Stream outputStream;
	
	/*
	 * Constructor
	 */
	public AbstractEntranceProcessingItem(EntranceProcessor processor) {
		this.processor = processor;
		this.parallelism = 1; // default to 1
	}
	
	/*
	 * Init & destroy
	 */
	public void onCreate() {
		// Do nothing
	}
	
	public void onDestroy() {
		// Do nothing
	}
	
	/*
	 * Processor
	 */
	public EntranceProcessor getProcessor() {
		return this.processor;
	}
	
	/*
	 * Parallelism 
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
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
	 * Output Stream
	 */
	public EntranceProcessingItem setOutputStream(Stream outputStream) {
		if (this.outputStream != null) {
			if (this.outputStream == outputStream) return;
			else
				throw new IllegalStateException("Cannot overwrite output stream of EntranceProcessingItem");
		}
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
			outputStream.put(processor.nextEvent());
			return true;
		}
		return false;
	}
}
