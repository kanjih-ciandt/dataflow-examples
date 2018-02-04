package kanjih.dataflow.examples;

import java.io.IOException;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class Gcs2PubSub {    

    public interface Pubsub2DatastoreOptions extends DataflowPipelineOptions {
        @Description("GCP project name")
        @Default.String("gcp_project_name")
        String getProjectId();

        void setProjectId(String value);
    }   

    public static void main(String[] args) throws IOException {

        Pubsub2DatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Pubsub2DatastoreOptions.class);

        Pipeline p = Pipeline.create(options);

        PCollection<String> line = p.apply("Read GCS", TextIO.read().from("gs://dataflowtestdatastore/process/*"));
        line.apply("Sending Pub/sub", PubsubIO.writeStrings().to("projects/spry-chassis-177914/topics/kiki"));

        PipelineResult result = p.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }
}
