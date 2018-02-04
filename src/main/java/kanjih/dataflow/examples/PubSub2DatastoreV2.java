package kanjih.dataflow.examples;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import kanjih.dataflow.examples.bo.MessageStore;

public class PubSub2DatastoreV2 {

    private static final Logger LOG = LoggerFactory.getLogger(PubSub2DatastoreV2.class);

    public interface Pubsub2DatastoreOptions extends DataflowPipelineOptions {
        @Description("Topic name of pub/sub")
        @Default.String("datastore")
        String getTopic();

        void setTopic(String value);

        @Description("Subscription name of pub/sub")
        @Default.String("datastore")
        String getSubscription();

        void setSubscription(String value);

        @Description("GCP project name")
        @Default.String("gcp_project_name")
        String getProjectId();

        void setProjectId(String value);

        @Description("Datastore kind name")
        @Default.String("test")
        String getKind();

        void setKind(String value);
    }

    public static class ParseMessageFn extends DoFn<String, MessageStore> {

        private static final long serialVersionUID = 7266270310828686837L;

        private static final Gson GSON = new GsonBuilder().create();

        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                LOG.info("Parsing msg : " + c.element());
                MessageStore msg = transform(c);
                c.output(msg);
            } catch (JsonParseException e) {
                System.err.printf("Bad JSON: %s\n", c.element().replaceAll("\n", ""));
            }
        }

        private MessageStore transform(ProcessContext c) {

            MessageStore msg = new MessageStore();

            Map<String, Object> map = GSON.fromJson(c.element(), new TypeToken<Map<String, Object>>() {
            }.getType());
            map.forEach((x, y) -> LOG.info("key : " + x + " , value : " + y));

            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) map.get("meta");
            LOG.info("meta:" + meta);

            @SuppressWarnings("unchecked")
            Map<String, Object> message = (Map<String, Object>) map.get("message");
            LOG.info("body:" + message);

            msg.setCreateDateTime((String) meta.get("X-KOHLS-CreateDateTime"));
            msg.setMessageType((String) meta.get("X-KOHLS-MessageType"));
            msg.setFromApp((String) meta.get("X-KOHLS-From-App"));
            msg.setCorrelationId((String) meta.get("X-KOHLS-CorrelationID"));

            msg.setStoreId((String) message.get("storeId"));
            msg.setItemId((String) message.get("itemId"));
            msg.setStockRoom((Integer) formatQtde(message.get("stockRoom")));
            msg.setSalesFloor((Integer) formatQtde(message.get("salesFloor")));
            msg.setStoreInProcess((Integer) formatQtde(message.get("storeInProcess")));
            msg.setEventDate((String) message.get("eventDateTime"));

            return msg;
        }

        private Integer formatQtde(Object qtde) {
            try {
                if (qtde == null) {
                    return null;
                }
                if (qtde instanceof String) {
                    return Ints.tryParse((String) qtde);
                } else if (qtde instanceof Integer) {
                    return (Integer) qtde;
                } else if (qtde instanceof Double) {
                    return ((Double) qtde).intValue();
                } else {
                    return null;
                }
            } catch (NumberFormatException ex) {
                return null;
            }
        }
    }
    
    public static class CreateEntityFn extends DoFn<MessageStore, KV<String,Entity>> {

        private static final long serialVersionUID = 8196953710138438427L;
        
        private final String kind;

        public CreateEntityFn(String kind) {
            this.kind = kind;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Entity.Builder entityBuilder = Entity.newBuilder();
            
            LOG.info("Converting datastore object " );

            MessageStore msg = c.element();
            
            LOG.info("msg:" + msg) ;  
            
            String key = msg.getStoreId() + msg.getItemId();
            Key.Builder keyBuilder = DatastoreHelper.makeKey(kind, key);
            entityBuilder.setKey(keyBuilder.build());
            
            putProperty(entityBuilder, "X-KOHLS-CreateDateTime", msg.getCreateDateTime(), false);
            putProperty(entityBuilder, "X-KOHLS-MessageType", msg.getMessageType(), false);
            putProperty(entityBuilder, "X-KOHLS-From-App", msg.getFromApp(), false);
            putProperty(entityBuilder, "X-KOHLS-CorrelationID", msg.getCorrelationId(), false);
            putProperty(entityBuilder, "storeId", msg.getStoreId(), false);
            putProperty(entityBuilder, "itemId", msg.getItemId(), false);
            putProperty(entityBuilder, "stockRoom", msg.getStockRoom(), false);
            putProperty(entityBuilder, "salesFloor", msg.getSalesFloor(), false);
            putProperty(entityBuilder, "storeInProcess", msg.getStoreInProcess(), false);
            putProperty(entityBuilder, "eventDateTime", msg.getEventDate(), false);
            
            entityBuilder.build();
            c.output(KV.of(key,  entityBuilder.build()));
        }
        
        private void putProperty(Entity.Builder entityBuilder, String name, String value, boolean indexed) {
            if(value == null ) {
                return;
            }
            if(indexed) {
                entityBuilder.putProperties(name, DatastoreHelper.makeValue(value).build());
            } else {
                entityBuilder.putProperties(name, DatastoreHelper.makeValue(value).setExcludeFromIndexes(true).build());
            }
        }
        
        private void putProperty(Entity.Builder entityBuilder, String name, Integer value, boolean indexed) {
            if(value == null ) {
                return;
            }
            if(indexed) {
                entityBuilder.putProperties(name, DatastoreHelper.makeValue(value).build());
            } else {
                entityBuilder.putProperties(name, DatastoreHelper.makeValue(value).setExcludeFromIndexes(true).build());
            }
        }

    }
    
    
    static class GroupEntityByKeyTransform extends PTransform<PCollection<KV<String, Entity>>, PCollection<Entity>> {

        private static final long serialVersionUID = -3918141442360295825L;

        @Override
        public PCollection<Entity> expand(PCollection<KV<String, Entity>> input) {

            PCollection<KV<String, Iterable<Entity>>> entityGroup = input.apply(GroupByKey.<String, Entity>create());           
           
            PCollection<Entity> stats = entityGroup.apply(ParDo.of(new DoFn<KV<String, Iterable<Entity>>, Entity>() {
                
                private static final long serialVersionUID = 2209844039157088145L;
                
                @ProcessElement
                public void processElement(ProcessContext c) throws IOException {
                    List<Entity> infoList = Lists.newArrayList(c.element().getValue());
                    //TODO: include logic to define which element with same key will be saved
                    c.output(infoList.get(0));
                }
            }));
           
            return stats;
        }

    }

    public static void main(String[] args) throws IOException {
        Pubsub2DatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Pubsub2DatastoreOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        String subscription = "projects/spry-chassis-177914/subscriptions/xpto1";
        
        LOG.info("Reading msg from pubsub ");

        PCollection<String> streamData = pipeline.apply("ReadingPubSub",
                PubsubIO.readStrings().fromSubscription(subscription));

        PCollection<String> intervalStreamData = streamData.apply("FixedWindow",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1L))));

        PCollection<MessageStore> parsedJson = intervalStreamData.apply("ParsingJson", ParDo.of(new ParseMessageFn()));

        PCollection<KV<String,Entity>> entities = parsedJson.apply("creatingEntityes",
                ParDo.of(new CreateEntityFn("xpto")));
        
        PCollection<KV<String, Entity>> windowsKey = entities.apply("WindowsKey",
                Window.<KV<String, Entity>>into(FixedWindows.of(Duration.standardMinutes(3L)))
                        .triggering(Repeatedly.forever(AfterWatermark.pastEndOfWindow()))
                        .withAllowedLateness(Duration.ZERO).discardingFiredPanes());

        PCollection<Entity> group = windowsKey.apply(new GroupEntityByKeyTransform());

        group.apply("SavingEntityes", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

}
