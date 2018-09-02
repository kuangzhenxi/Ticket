
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class WriterBolt extends BaseBasicBolt{

	FileWriter fileWriter = null;
	/**
	 * bolt组件运行过程中只会调用一次，就第一次生成实例时调用
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		try {
			fileWriter = new FileWriter("c://kzx/kafkastormData/"+UUID.randomUUID());
//			fileWriter = new FileWriter("/test/kafkastorm"+UUID.randomUUID());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		
		List list = input.getValues();
		HashMap<String,String> map = (HashMap<String,String>) list.get(0);
		try {
			for(Map.Entry entry: map.entrySet())
	        {
				
				String s = (String)entry.getKey();
				if((s!=null) &&(!s.equals("无")))
				{
					System.out.println("Key: "+ entry.getKey()+ " Value: "+entry.getValue());
					fileWriter.write(entry.getKey()+"  :  "+entry.getValue()+"\n");
				}
	        }
			
			fileWriter.write("\n-------------------------\n");
			fileWriter.flush();
		} catch(IOException e) {
			e.printStackTrace();
		}
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
