package com.serendio.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.server.BayeuxServer;
import org.cometd.bayeux.server.ServerMessage;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;

public class PublisherClient extends Thread {

	
	 public static void main(String[] args) throws IOException
	    {
			 String url = "http://localhost:8080/cometd/";
			 //String url = "http://78.46.77.101:8080/cometd/";
			 BayeuxClient client = new BayeuxClient(url, LongPollingTransport.create(null));
			 client.handshake();
			 client.waitFor(1000, BayeuxClient.State.CONNECTED);
	
			 // Subscription to channels			 
			 ClientSessionChannel channel = client.getChannel("/itb/mydemo");

			 Map<String, Object> data = new HashMap<String, Object>();
	         data.put("user", "Publishing Client");
	         for (int i = 0; i < 2;i++){
	        	 try {
					Thread.sleep(200);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		         data.put("itb", System.currentTimeMillis());         
				 channel.publish(data);
	         }
	
	        channel.unsubscribe(); 
		    client.disconnect();
	        client.waitFor(1000, BayeuxClient.State.DISCONNECTED);
        }

}
