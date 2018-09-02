
public class TestDeleteTopic {
	
	public static void main(String[] args) {
        
        //zookeeper地址：端口号
        String ZkStr = "hadoop1:2181";    
        
        //topic对象
//        KafkaTopicBean topic = new KafkaTopicBean();    
//        topic.setTopicName("testTopic");  //topic名称        
//        topic.setPartition(1);            //分区数量设置为1
//        topic.setReplication(1);        　//副本数量设置为1
        


        
        //删除topic
        KafkaUtil.deleteKafaTopic(ZkStr);
        //创建topic
        KafkaUtil.createKafaTopic(ZkStr);
        
        
    }
}
