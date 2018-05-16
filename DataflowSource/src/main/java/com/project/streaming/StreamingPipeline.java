package com.project.streaming;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class StreamingPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingPipeline.class);

  public static void main(String[] args) {
	  StreamingOptions options = PipelineOptionsFactory.as(StreamingOptions.class);
	  options.setProject("serene-circlet-189907");
	  options.setTempLocation("gs://temp-junk/temp/");
	  options.setStagingLocation("gs://temp-junk/staging/");
	  options.setStreaming(true);
	  options.setRunner(DataflowRunner.class);
	  options.setJobName("streaming-job");
	  options.setTemplateLocation("gs://streaming-solutions/source_code/dataflow_template/StreamingTemplate");
	  
	  Pipeline p = Pipeline.create(options);
	  
	  PCollection<PubsubMessage> streamingData = p.apply("StreamMessages",PubsubIO.readMessagesWithAttributes()
			  .fromSubscription("projects/serene-circlet-189907/subscriptions/data_endpoint"));
	  
	  PCollection<PubsubMessage>windowdStreams = streamingData.apply(
			    "GatherMessages",Window.<PubsubMessage>into(FixedWindows.of(Duration.standardMinutes(1))));
	  
	  windowdStreams.apply("BQ_WriteToTarget",BigQueryIO.<PubsubMessage>write()
			  .withFormatFunction(new SerializableFunction<PubsubMessage,TableRow>(){
				private static final long serialVersionUID = 1L;

				@Override
				public TableRow apply(PubsubMessage message) {
					// fetch payload from message
					String streamingPayload = new String(message.getPayload());
					String[] columnarData = streamingPayload.split(",");
					
					// convert payload to BQ friendly format
					TableRow writeRow = new TableRow();
					String schema = message.getAttribute("schema");
					String[] columns = schema.split(",");
					
					for(int index=0; index < columns.length; index++) {
						String columnName = columns[index].split(":")[0];
						writeRow.set(columnName, columnarData[index]);
					}
					LOG.info(writeRow.toString());
					return writeRow;
				}
			  })
		.to(new DynamicDestinations<PubsubMessage, PubsubMessage>(){
			private static final long serialVersionUID = 1L;

			@Override
			public PubsubMessage getDestination(ValueInSingleWindow<PubsubMessage> element) {
				return element.getValue();
			}

			@Override
			public TableSchema getSchema(PubsubMessage destination) {
				String schema = destination.getAttribute("schema");
				String[] columns = schema.split(",");
				List<TableFieldSchema> fields = new ArrayList<>();
				for(String column : columns) {
					LOG.info(column);
					String[] columnInfo = column.split(":");
					fields.add(new TableFieldSchema().setName(columnInfo[0]).setType(columnInfo[1]));
				}
				return new TableSchema().setFields(fields);
			}

			@Override
			public TableDestination getTable(PubsubMessage destination) {
				String tableDescription = destination.getAttribute("destination");
				return new TableDestination(tableDescription, "PubSubWrites");
			}

	  }).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
	  
	  p.run().waitUntilFinish();
	  
  }
}