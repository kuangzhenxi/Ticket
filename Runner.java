

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import storm2kafka.bolt.WordSpliter;
import storm2kafka.bolt.WriterBolt;
import storm2kafka.utils.KafkaUtil;
import storm2kafka.utils.MessageScheme;

public class KafkaTopo {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
		String topic = "testFlume3";
		String zkRoot = "/kafka-storm3";
		String spoutId = "KafkaSpout1";
		BrokerHosts brokerHosts = new ZkHosts("hadoop1:2181,hadoop2:2181");
		
		
		
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
//		spoutConfig.fetchMaxWait=120000;
//		spoutConfig.forceFromStart = true;
		spoutConfig.ignoreZkOffsets = true; //跟上面一样，版本不同
		spoutConfig.scheme = new SchemeAsMultiScheme(new MessageScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(spoutId, new KafkaSpout(spoutConfig)); //spout已经整合写好了
		
		
		builder.setBolt("word-spilter", new WordSpliter()).shuffleGrouping(spoutId);
		builder.setBolt("writer", new WriterBolt(),1).fieldsGrouping("word-spilter", new Fields("word"));
		
		Config conf = new Config();
		
		conf.setNumWorkers(1);
//		conf.setNumAckers(0);
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("testFlumeTopo1", conf, builder.createTopology());
//		StormSubmitter.submitTopology("testFlumeTopo1", conf, builder.createTopology());
		
	}

}
