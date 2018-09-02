

import java.util.Properties;
import org.apache.kafka.common.security.JaasUtils;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;

public class KafkaUtil {
    
     public static void createKafaTopic(String ZkStr) {  
         ZkUtils zkUtils = ZkUtils.
                 apply(ZkStr, 30000, 30000,JaasUtils.isZkSecurityEnabled()); 
         
        AdminUtils.createTopic(zkUtils, "testFlume3",  1, 1,  new Properties(), new RackAwareMode.Enforced$());  
        zkUtils.close();
    }
     
     public static void deleteKafaTopic(String ZkStr) {  
         ZkUtils zkUtils = ZkUtils.
                 apply(ZkStr, 30000, 30000,JaasUtils.isZkSecurityEnabled()); 
        
//         zkUtils.deletePathRecursive(ZkUtils.getTopicPath("testFlume"));
        AdminUtils.deleteTopic(zkUtils, "testFlume3");  
        zkUtils.close();
    }

}
