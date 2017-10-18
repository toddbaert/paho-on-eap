package org.outsrcd.sample.paho;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.ContextStoppedEvent;
import org.springframework.stereotype.Component;

@Component
public class PahoDemo implements ApplicationListener<ApplicationEvent> {

  Thread pahoThread;
  PahoRunnable pahoRunnable;

  @Override
  public void onApplicationEvent(ApplicationEvent event) {

    System.out.println(event.toString());

    if (event.getClass().equals(ContextRefreshedEvent.class)) {

      pahoRunnable = new PahoRunnable();
      pahoThread = new Thread(pahoRunnable);
      pahoThread.start();

    } else if (event.getClass().equals(ContextStoppedEvent.class)
        || event.getClass().equals(ContextClosedEvent.class)) {

      if (pahoThread != null) {
        try {
          pahoRunnable.stop();
          pahoThread.join();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
