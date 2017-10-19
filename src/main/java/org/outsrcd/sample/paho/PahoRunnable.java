package org.outsrcd.sample.paho;

import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class PahoRunnable implements Runnable{  
  
  String TOPIC = "demo.topic";
  MqttConnectOptions connOpts;
  MqttClient sampleClient;
  volatile boolean stopped = false;
  String[] serverUrls = new String[] { "ssl://localhost:1883", "ssl://localhost:1884" };
  
  // callback to fire on received messages
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
    
    Properties sslProps = new Properties();
    sslProps.setProperty("com.ibm.ssl.keyStore", "/path/to/client-keystore.jks");
    sslProps.setProperty("com.ibm.ssl.keyStorePassword", "password");
    sslProps.setProperty("com.ibm.ssl.keyStoreType", "JKS");
    
    // set clean session to false, and let paho attempt to connect to any configured url
    connOpts = new MqttConnectOptions();
    connOpts.setCleanSession(false);
    connOpts.setServerURIs(serverUrls);
    connOpts.setSSLProperties(sslProps);

    try {
    
      while (!stopped) {
        
          if (sampleClient == null || !sampleClient.isConnected()) {
            connect();
            sampleClient.subscribe(TOPIC);
          }
          
          // generate a random int to publish
          int randomInt = ThreadLocalRandom.current().nextInt(0, 100);
          MqttMessage message = new MqttMessage(String.valueOf(randomInt).getBytes());
          message.setQos(1);

          // publish
          sampleClient.publish(TOPIC, message);
          System.out.println(">>> published: " + randomInt);   
          
          // wait 2 seconds
          Thread.sleep(2000);
      }
    } catch (MqttException me) {
      System.out.println(">>> error: " + me.getMessage());   
    } catch (InterruptedException e) {
      stop();
    }
  }
  
  // try to reconnect in a loop. Paho client will utilized defined list of hosts
  private void connect() throws InterruptedException {
    boolean connecting = true;
    
    while (connecting && !stopped) {
      try {
        
        if (sampleClient == null) {
          sampleClient = new MqttClient("ssl://localhost:1883", "paho-demo");
          sampleClient.setCallback(callback);
        }       

        // connect will attempt to connect to any of the URLs defined in serverUrls
        sampleClient.connect(connOpts);
        connecting = false;
      } catch (MqttException e) {
        System.out.println(">>> unable to connect to any configured broker: ");
        System.out.println(e.getMessage());

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
        System.out.println(">>> error: " + e.getMessage());   

      }
    }  
  }
}
