package org.outsrcd.sample.paho;

import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PahoRunnable implements Runnable{  
  
  String topic = "demo.topic";
  MemoryPersistence persistence = new MemoryPersistence();
  MqttConnectOptions connOpts;
  MqttClient sampleClient;
  volatile boolean stopped = false;
  String[] serverUrls = new String[] { "ssl://localhost:1883", "ssl://localhost:1884" };
  
  // callback to fire on receive messages
  MqttCallback callback = new MqttCallback() {

    public void connectionLost(Throwable arg0) {
      System.out.println(">>> connection lost");
    }

    public void deliveryComplete(IMqttDeliveryToken arg0) {

    }

    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
      System.out.println(">>> message arrived: " + arg1);
    }

  };
  
  @Override
  public void run() {

    System.out.println(">>> running paho demo...");
    
    // set clean session to false, and let paho attempt to connect to any configured url
    connOpts = new MqttConnectOptions();
    connOpts.setCleanSession(false);
    connOpts.setServerURIs(serverUrls);
  
    try {
      sampleClient = new MqttClient("ssl://localhost:1883", "paho-demo", persistence);
      sampleClient.setCallback(callback);
    } catch (MqttException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    try {
    
      while (!stopped) {
        
          if (!sampleClient.isConnected()) {
            connect();
            sampleClient.subscribe(topic);
          }
          
          // generate a random int to publish
          int randomInt = ThreadLocalRandom.current().nextInt(0, 100);
          MqttMessage message = new MqttMessage(String.valueOf(randomInt).getBytes());
          message.setQos(1);

          // publish
          sampleClient.publish(topic, message);
          System.out.println(">>> published: " + randomInt);   
          
          // wait 2 seconds
          Thread.sleep(2000);
      }
    } catch (MqttException me) {
      me.printStackTrace();
    } catch (InterruptedException e) {
      stop();
    }
  }
  
  // try to reconnect in a loop. Paho client will utilized defined list of hosts
  private void connect() throws InterruptedException {
    boolean connecting = true;
    
    while (connecting && !stopped) {
      try {
        sampleClient.connect(connOpts);
        connecting = false;
      } catch (MqttException e) {
        System.out.println(">>> unable to connect to any configured broker");
        Thread.sleep(2000);
      }
    }
    System.out.println(">>> reconnected!");   
  }
  
  // stop the runnable, being sure to close the connection
  public void stop() {
    System.out.println(">>> stopped");   
    stopped = true;
    if (sampleClient != null)
    {
      try {
        sampleClient.close();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }  
  }
}
