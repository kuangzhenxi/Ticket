

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordSpliter extends BaseBasicBolt{
	
	private HashMap<String,String> map = new HashMap<String,String>();
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		map.put("长沙", "无");
		map.put("株洲", "无");
		map.put("湘潭", "无");
		map.put("衡阳", "无");
		map.put("邵阳", "无");
		map.put("岳阳", "无");
		map.put("常德", "无");
		map.put("张家界", "无");
		map.put("益阳", "无");
		map.put("郴州", "无");
		map.put("永州", "无");
		map.put("怀化", "无");
		map.put("娄底", "无");
		map.put("吉首", "无");
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		String[] words = line.split(",");
		String to_location = words[3].replace("\"", "");
		String to_head = "";
		if(to_location.startsWith("长沙")) {
			to_head = "长沙";
		}else if(to_location.startsWith("株洲")) {
			to_head = "株洲";
		}else if(to_location.startsWith("湘潭")) {
			to_head = "湘潭";
		}else if(to_location.startsWith("衡阳")) {
			to_head = "衡阳";
		}else if(to_location.startsWith("邵阳")) {
			to_head = "邵阳";
		}else if(to_location.startsWith("岳阳")) {
			to_head = "岳阳";
		}else if(to_location.startsWith("常德")) {
			to_head = "常德";
		}else if(to_location.startsWith("张家界")) {
			to_head = "张家界";
		}else if(to_location.startsWith("益阳")) {
			to_head = "益阳";
		}else if(to_location.startsWith("郴州")) {
			to_head = "郴州";
		}else if(to_location.startsWith("永州")) {
			to_head = "永州";
		}else if(to_location.startsWith("怀化")) {
			to_head = "怀化";
		}else if(to_location.startsWith("娄底")) {
			to_head = "娄底";
		}else if(to_location.startsWith("吉首")) {
			to_head = "吉首";
		}
//		System.out.println(to_head+"------------");
		for(int i=7; i<=14; ++i)
		{
			String str= words[i].replace("\"", "");
			String result = map.get(to_head);
//			System.out.println(to_head+"+++++++++++++   "+result+"----------------");
			if(result.equals("有"))
				break;
			if(str.equals("有"))
			{
				map.put(map.get(to_head),"有");
				break;
			}
			if((!str.equals("无")) &&(!str.equals("有")) && (!str.equals("")))
			{
				map.put(map.get(to_head),"中");
			}
			
		}
		collector.emit(new Values(map));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
