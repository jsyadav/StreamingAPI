package com.serendio.streaming;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

public class PubSubClient {

	public static void main(String[] args) throws IOException {
		int numOfThreads = 10;
		MyThread[] threads = new MyThread[numOfThreads];

		for (int i = 0; i < numOfThreads; i++) {
			threads[i] = new MyThread();
			threads[i].run();
		}

	}


	public static class MyThread {//implements Runnable {
		public int count = 0;
		public int limit = 2;
		public int delay = 1;
		BayeuxClient client;
		
	    int pubCnt = 0;
		
		// String url = "http://localhost:8080/cometd";
		String url = "http://78.46.77.101:8080/cometd";
		String endPoint = "/itb/mydemo"; // "/service/echo"
		// String endPoint = "/service/echo";

		MyThread() {

			Map<String,Object> options = new HashMap<String,Object>();
			options.put(ClientTransport.INTERVAL_OPTION, 50000);
			HttpClient httpClient = new HttpClient();
	        httpClient.setIdleTimeout(50000);
	        httpClient.setConnectorType(HttpClient.CONNECTOR_SELECT_CHANNEL);
	        httpClient.setMaxConnectionsPerAddress(32768);
	        
			client = new BayeuxClient(url, LongPollingTransport.create(null));
			client.handshake();
			client.waitFor(1000, BayeuxClient.State.CONNECTED);	

			/*
			client.getChannel(Channel.META_CONNECT).addListener(new ClientSessionChannel.MessageListener() {
				
				public void onMessage(ClientSessionChannel arg0, Message arg1) {
					// TODO Auto-generated method stub
					Map<String,Object> adv = arg1.getAdvice();
					if (adv != null)
					System.out.println("interval is "+ adv.containsValue("interval"));
					else
						System.out.println(new Date().toLocaleString());
				}
			});*/
		}
		

		public void run() {
			publish();
			
			
			// Subscription to channels
			ClientSessionChannel channel = client.getChannel(endPoint);

			channel.subscribe(new ClientSessionChannel.MessageListener() {
				public void onMessage(ClientSessionChannel channel,
						Message message) {
					count++;
					// Handle the message
					//System.out.println("Received message " + message.toString()
					//		+ " count " + count +", threadid "+ Thread.currentThread().getId());
					//Object[] posts = (Object[]) message.getData();

					// dump(message);
					if (count == limit) {
						publish();
						count = 0;
						// channel.unsubscribe();
						// client.disconnect();
					}
				}

			});
		}

		public void publish() {
			// Publishing to service/itb
			if (pubCnt++%limit == 0)
			 System.out.println("Publish cnt "+ pubCnt + " threadid "+ Thread.currentThread().getId() + ", Date " + new Date().toLocaleString());
			Map<String, Object> data = new HashMap<String, Object>();
			data.put("room", endPoint); // Listening channel
			data.put("phrase", "ford");
			data.put("limit", String.valueOf(limit));
			data.put("delay", String.valueOf(delay));
			// data.put("type", "xml");
			// data.put("validity", "3600");
			ClientSessionChannel clt = client.getChannel("/service/itb");
			// ClientSessionChannel clt = client.getChannel(endPoint);
			clt.publish(data);
		}

		// client.disconnect();
		// client.waitFor(1000, BayeuxClient.State.DISCONNECTED);

	}

	public static void dump(Message message) {

		Object obj = message.getData();
		Object o[] = (Object[]) obj;

		for (int i = 0; i < o.length; i++) {
			Map<String, String> da = (Map<String, String>) o[i];
			Set<Entry<String, String>> set = da.entrySet();
			for (Entry<String, String> e : set) {
				System.out.println(e.getKey() + " : " + e.getValue());
			}
		}

	}

}
