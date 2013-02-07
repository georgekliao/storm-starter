package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.spout.GeorgeWordSpout;
import storm.starter.spout.RandomSentenceSpout;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopologySlow {

    public static Logger LOG = LoggerFactory.getLogger(GeorgeWordSpout.class);
    
    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            LOG.info("Presleep preemit");
            Utils.sleep(1000);
            LOG.info("Postsleep preemit");
            
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));

            LOG.info("Presleep postemit");
            Utils.sleep(1000);
            LOG.info("Postsleep postemit");
            
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


    }
    
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("word", new GeorgeWordSpout(), 5);
        builder.setBolt("exclaim1", new ExclamationBolt(), 3)
                .shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
                .shuffleGrouping("exclaim1");
                
        Config conf = new Config();
        conf.setDebug(true);
        conf.setMaxSpoutPending(10);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}
