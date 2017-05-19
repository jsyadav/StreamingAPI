package com.serendio.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;

public class SubscriberClient {
	static int count =0;
	 public static void main(String[] args) throws IOException
	    {
	      	String url = "http://localhost:8080/cometd";
		    //String url = "http://78.46.77.101:8080/cometd";
			 BayeuxClient client = new BayeuxClient(url, LongPollingTransport.create(null));
			 client.handshake();
			 client.waitFor(1000, BayeuxClient.State.CONNECTED);
			 System.out.println("Client id "+ client.getId());



			 // Subscription to channels
			 ClientSessionChannel channel = client.getChannel("/itb/mydemo");
			 //ClientSessionChannel channel = client.getChannel("/service/echo");
			 channel.subscribe(new ClientSessionChannel.MessageListener()
			 {
			     public void onMessage(ClientSessionChannel channel, Message message)
			     {
			    	 count++;
			         // Handle the message
			    	 System.out.println("Received message " + message.toString()+  " count " + count);
			    	 
			     }
			 });

			 	
	    }
}
