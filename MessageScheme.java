

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class MessageScheme implements Scheme {

	@Override
	public List<Object> deserialize(ByteBuffer ser) {

		try {
			byte[] b = new byte[ser.remaining()];
			ser.get(b,0,b.length);
            String msg = new String(b, "UTF-8");
            return new Values(msg);
        } catch (UnsupportedEncodingException ignored) {
        	ignored.printStackTrace();        
        }
		return null;
		
	}
	@Override
	public Fields getOutputFields() {
		return new Fields("msg");
	}
	

}
