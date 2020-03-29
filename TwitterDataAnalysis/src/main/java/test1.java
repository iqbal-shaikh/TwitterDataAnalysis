import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class test1 {

	public static void main(String[] args) throws Exception{
	      
	      //Assign topicName to string variable
	      String topicName = "test";
	      
	      // create instance for properties to access producer configs   
	      Properties props = new Properties();
	      
	      //Assign localhost id
//	      props.put("bootstrap.servers", "172.16.7.203:6667");
	      props.put("bootstrap.servers", "127.0.0.1:9092");
	      
	      //Set acknowledgements for producer requests.      
	      props.put("acks", "all");
	      
	      
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      Producer<String, String> producer = new KafkaProducer
	         <String, String>(props);
	            
	     

	               try {
	            	   for(int i = 0; i < 10; i++)
	          	         producer.send(new ProducerRecord<String, String>(topicName, 
	          	            Integer.toString(i), Integer.toString(i)));
	          	               System.out.println("Message sent successfully");
	          	               producer.close();
	               } catch (Exception ex) {
	            	   System.err.println("An InterruptedException was caught: " + ex.getMessage());
	                   // handle the exception
	               }
	   }
}
