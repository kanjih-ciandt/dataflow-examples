package kanjih.dataflow.examples;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;

import kanjih.dataflow.examples.bo.Message;

public class PubSub2Datastore {
    
    private static final Logger LOG = LoggerFactory.getLogger(PubSub2Datastore.class);

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
    
    
    public static class ParseMessageFn extends DoFn<String, Message>  {

        private static final long serialVersionUID = 7266270310828686837L;
      
        private static final Gson GSON = new GsonBuilder().create();
        
        @ProcessElement
        public void processElement(ProcessContext c) {
            try {
                LOG.info("Parsing msg : " + c.element());
                Message msg = transform(c);
                c.output(msg);
            } catch (JsonParseException e) {
                System.err.printf("Bad JSON: %s\n", c.element().replaceAll("\n", ""));
            }
        }

        private Message transform(ProcessContext c) {
           
            Message msg = new Message();
            
            Map<String, Object> map = GSON.fromJson(c.element(), new TypeToken<Map<String, Object>>(){}.getType());
            map.forEach((x,y)-> LOG.info("key : " + x + " , value : " + y));
            
            @SuppressWarnings("unchecked")
            Map<String, Object> header =  (Map<String, Object>) map.get("Header");
            LOG.info("header:" + header);
            
            @SuppressWarnings("unchecked")
            Map<String, Object> body =  (Map<String, Object>) map.get("Body");
            LOG.info("body:" + body);
            
            String createDate = (String) header.get("createDate");
            msg.setCreateDate(createDate);
            String from =  (String) header.get("from");
            msg.setFrom(from);
            String msgId =   (String) header.get("msgId");
            msg.setMsgId(msgId);
            String msgType =  (String) header.get("msgType");
            msg.setMsgType(msgType);
            
            String storeId = (String) body.get("storeId");
            msg.setStoreId(storeId);
            String productId = (String) body.get("productId");
            msg.setProductId(productId);
            
            String dateProcessing = (String) body.get("createDate");
            if (null != dateProcessing) {
                msg.setEventDateTime(dateProcessing);
            }
           
            msg.setaQtdy(formatQtde(body.get("aQtdy")));
            Object b = body.get("bQtdy");
                            
            LOG.info("b:" +  b.getClass());
            msg.setbQtdy(formatQtde(body.get("bQtdy")));
            msg.setcQtdy(formatQtde(body.get("cQtdy")));
            msg.setdQtdy(formatQtde(body.get("dQtdy")));
            
            
            LOG.info("msg:" + msg);
            
            return msg;
        }
        
        private Integer formatQtde(Object qtde) {
           try {
            if(qtde == null) {
                return null;
            }
            
            if(qtde instanceof String) {
                return Ints.tryParse((String) qtde);
            } else if (qtde instanceof Integer) {
                return (Integer) qtde;
            } else if (qtde instanceof Double) {
                return ((Double) qtde).intValue();
            } else {
                return null;
            }
           }catch (NumberFormatException ex) {
               return null;
           }
        }
    }

    public static class CreateEntityFn extends DoFn<Message, Entity> {

        private static final long serialVersionUID = 8196953710138438427L;
        

        private final String kind;

        public CreateEntityFn(String kind) {
            this.kind = kind;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            Entity.Builder entityBuilder = Entity.newBuilder();
            
            LOG.info("Converting datastore object " );

            Message msg = c.element();
            
            LOG.info("msg:" + msg) ;  
            
            String key = UUID.randomUUID().toString();
            Key.Builder keyBuilder = DatastoreHelper.makeKey(kind, key);
            entityBuilder.setKey(keyBuilder.build());
            
            putProperty(entityBuilder, "qtdyA", msg.getaQtdy(), false);
            putProperty(entityBuilder, "qtdyB", msg.getbQtdy(), false);
            putProperty(entityBuilder, "qtdyC", msg.getcQtdy(), false);
            putProperty(entityBuilder, "qtdyD", msg.getdQtdy(), false);
            putProperty(entityBuilder, "createDate", msg.getEventDateTime(), true);
            putProperty(entityBuilder, "from", msg.getdQtdy(), false);
            putProperty(entityBuilder, "msgId", msg.getMsgId(), true);
            putProperty(entityBuilder, "msgType", msg.getMsgType(), false);
            putProperty(entityBuilder, "storeId", msg.getStoreId(), true);
            putProperty(entityBuilder, "productId", msg.getProductId(), true);
            entityBuilder.build();
            c.output(entityBuilder.build());
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

    public static void main(String[] args) throws IOException{
        Pubsub2DatastoreOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(Pubsub2DatastoreOptions.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        String subscription = "projects/" + options.getProject() + "/subscriptions/" + options.getSubscription();
        LOG.info("Reading msg from pubsub ");

        PCollection<String> streamData = pipeline.apply("ReadingPubSub",
                PubsubIO.readStrings().fromSubscription(subscription));

        PCollection<String> intervalStreamData = streamData.apply("FixedWindow",
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1L))));

        PCollection<Message> parsedJson = intervalStreamData.apply("ParsingJson", ParDo.of(new ParseMessageFn()));
    
        PCollection<Entity> entities = parsedJson.apply("creatingEntityes",
                ParDo.of(new CreateEntityFn(options.getKind())));

        entities.apply("SavingEntityes", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }

    }

}
