package com.project.streaming;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface StreamingOptions extends DataflowPipelineOptions {
	public void setTopicName(ValueProvider<String> value);
	public ValueProvider<String> getTopicName();
}
