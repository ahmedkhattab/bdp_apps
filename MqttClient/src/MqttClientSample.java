import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.JSONObject;

public class MqttClientSample {
	public static JSONObject getNewReading(String device) {
		Random r = new Random();

		JSONObject reading = new JSONObject();
		reading.put("device", device);
		reading.put("value", r.nextInt(10) + r.nextFloat());
		reading.put("timestamp", System.currentTimeMillis());
		System.out.println(reading.toString());
		return reading;
	}

	public static void main(String[] args) {
		String[] devices = {"s1", "s2"};
		int qos = 1;
		String broker = String.format("tcp://52.29.211.135:31315");
		String clientId = "JavaSample";
		MemoryPersistence persistence = new MemoryPersistence();

		try {
			MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
			MqttConnectOptions connOpts = new MqttConnectOptions();
			connOpts.setCleanSession(true);
			System.out.println("Connecting to broker: " + broker);
			sampleClient.connect(connOpts);
			System.out.println("Connected");
			for (int i = 0; i < 50; i++) {
				System.out.println("Publishing message: " + getNewReading(devices[i%2]));
				MqttMessage message = new MqttMessage(getNewReading(devices[i%2]).toString().getBytes());
				message.setQos(qos);
				sampleClient.publish(devices[i%2], message);
				System.out.println("Message published");
				Thread.sleep(1000);
			}
			sampleClient.disconnect();
			System.out.println("Disconnected");
			System.exit(0);
		} catch (MqttException me) {
			System.out.println("reason " + me.getReasonCode());
			System.out.println("msg " + me.getMessage());
			System.out.println("loc " + me.getLocalizedMessage());
			System.out.println("cause " + me.getCause());
			System.out.println("excep " + me);
			me.printStackTrace();
		} catch (InterruptedException e) {
			System.out.println("reason " + e.getMessage());
			e.printStackTrace();
		}
	}
}