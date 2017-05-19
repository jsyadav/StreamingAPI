/*
 * Copyright (c) 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 *
 */
package com.serendio.streaming;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;

import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.server.BayeuxServer;
import org.cometd.bayeux.server.ConfigurableServerChannel;
import org.cometd.bayeux.server.ServerChannel;
import org.cometd.bayeux.server.ServerMessage;
import org.cometd.bayeux.server.ServerSession;
import org.cometd.bayeux.server.ServerMessage.Mutable;
import org.cometd.java.annotation.Configure;
import org.cometd.java.annotation.Listener;
import org.cometd.java.annotation.Service;
import org.cometd.java.annotation.Session;
import org.cometd.server.ServerSessionImpl;
import org.cometd.server.authorizer.GrantAuthorizer;
import org.cometd.server.filter.DataFilter;
import org.cometd.server.filter.DataFilterMessageListener;
import org.cometd.server.filter.JSONDataFilter;
import org.cometd.server.filter.NoMarkupFilter;
import org.json.JSONException;
import org.json.XML;

import com.serendio.connector.hbase.SearchHBase;

@Service("itb")
public class StreamingService
{
    private final ConcurrentMap<String, Map<String, String>> _members = new ConcurrentHashMap<String, Map<String, String>>();
   
    @Inject
    private BayeuxServer _bayeux;
    @Session
    private ServerSession _session;


    @Configure ({"/itb/**"})
    protected void configureItbStarStar(ConfigurableServerChannel channel)
    {
    	//System.out.println("Configure channel" + channel.getId() + " " + this);
        DataFilterMessageListener noMarkup = new DataFilterMessageListener(new NoMarkupFilter(),new BadWordFilter());
        channel.addListener(noMarkup);
        channel.addAuthorizer(GrantAuthorizer.GRANT_ALL);
        channel.setPersistent(true);
        
    	_session.addListener(new ServerSession.RemoveListener() {			
			public void removed(ServerSession arg0, boolean arg1) {
				// TODO Auto-generated method stub
				System.out.println("Something removed..");
			}
		});
    }
    
    @Listener({Channel.META_CONNECT})
    protected void connect(ServerSession client, ServerMessage message) {
    	System.out.println("META_CONNECT request from client "+ client.getId() + " with message "+ message);      	
    	if (!client.isConnected()) {
        	System.out.println("Client " + client.getId() + " is not connected");        	
    	}    	    		
    }
    
    
    @Listener({Channel.META_DISCONNECT})
    protected void disconnect(ServerSession client, ServerMessage message) {
    	System.out.println("META_DISCONNECT request from client "+ client.getId() + " with message "+ message);
    }
    
    @Listener({Channel.META_SUBSCRIBE})
    protected void subscrbe(ServerSession client, ServerMessage message) {
    	System.out.println("META_SUBSCRIBE request from client "+ client.getId() + " with message "+ message);
    }
    
    @Listener({Channel.META_UNSUBSCRIBE})
    protected void unsubscribe(ServerSession client, ServerMessage message) {
    	System.out.println("META_UNSUBSCRIBE request from client "+ client.getId() + " with message "+ message);
    }
    
    @Listener({Channel.META_HANDSHAKE})
    protected void handshake(ServerSession client, ServerMessage message) {
    	System.out.println("META_HANDSHAKE request from client " + message );
    }
    
    /*
     * You want the broadcast messages to be handled by the server only.
     */
    
    @Listener({"/service/itb"})
    protected void handleItbMessage(final ServerSession client, final ServerMessage message)
    {
    	//System.out.println("handle itb a service message " + this.toString());
    	//System.out.println("Arg session is " + client.toString());
    	//System.out.println("Member server session is "+ _session.toString());
    	//System.out.println("Member server local session is "+ _session.getLocalSession().toString());
    	//System.out.println("is arg local session " + client.isLocalSession());
    	//System.out.println("is member server local session " + _session.isLocalSession());

    
        System.out.println("Publish Message "+ "\""+message.toString()+"\", from client \""+
        		client.getId() + "\", server \""+ _session.getId() +"\" on threadId " +Thread.currentThread().getId());
        
        handleMembership(client, message);
        
    	new Thread() {
    		public void run() {
	        	boolean HBase = true;

	        	Map<String,Object> data = message.getDataAsMap();
    	    	String exp =(String)data.get("phrase");
    	    	
    	    	// Default settings
    	    	String type = (String) (data.get("type") != null?data.get("type"):"json");	    	    	
    			int resultLimit = data.get("limit") != null?Integer.valueOf((String)data.get("limit")):10;
    	    	int delay = data.get("delay") != null?Integer.valueOf((String)data.get("delay")):2;
    	    	int validity = data.get("validity") != null?Integer.valueOf((String)data.get("validity")):0;

    			if (delay < 0) {
    				System.out.println ("Delay value can't negative, passed val is  "+ delay );
    				return ;
    			}
    			
	        	if (HBase) {	  
	        		// TODO Hard coded the table name for now
		        	SearchHBase search = new SearchHBase("test");  
	    			List<Map<String,Object>> posts = search.searchByExpression(exp, resultLimit, type, validity);
	    			if (posts.size() != 0) {
		    			for (Map<String,Object> post : posts) {
		    				try {
		    					if (delay > 0) {
		    						deliverMessage(client, message, post);
		    						Thread.sleep(delay*1000);
		    					}	
		    				} catch (InterruptedException e) {
		    					// TODO Auto-generated catch block
		    					e.printStackTrace();
		    				}
		    			}		
	    			}else {
	    				// When no post is found.
	    				deliverMessage(client, message, null);
	    			}
	        	}else {    		
	        		String output = "<Sentiment_Analysis> <language>en</language>" +
	        		"<product_name>ford focus</product_name>" +
	        		"<weighted_score>0</weighted_score>" +
	        		"<unweighted_score>0</unweighted_score>" +
	        		"<Features>" +
	        		"<feature>" +
	        		"<feature_name>unknown feature</feature_name>" +
	        		"<sentiment>positive</sentiment>" +
	        		"<negative_probability>0</negative_probability>" +
	        		"<negative_words/>" +
	        		"<positive_probability>1</positive_probability>" +
	        		"<positive_words>sentimentword, pleas</positive_words>" +
	        		"</feature>" +
	        		"</Features>" +
	        		"<timestamp>" +
	        		"<exit_timestamp>15:25:57.356957</exit_timestamp>" +
	        		"<start_timestamp>15:25:57.321322</start_timestamp>" +
	        		"<elapsed_time>35635</elapsed_time>" +
	        		"</timestamp>" +
	        		"<ElapsedTime>483.0 msec</ElapsedTime>" +
	        		"</Sentiment_Analysis>";
	        		Map<String, Object> post = new HashMap<String, Object>();
	        		if (!type.equalsIgnoreCase("json")) {
						post.put("Sentiment_Analysis", output);
					}else {
						try {
							post.put("Sentiment_Analysis", XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
	        		
	        		deliverMessage(client, message, post);
	        	}	
    		}
    	}.start();
    	
        
    }
    
    
    public void deliverMessage(ServerSession client, Message msg, Map<String, Object> post)
    {
    	//System.out.println(msg.toString());
        Map<String,Object> data = msg.getDataAsMap();
    	String room = ((String)data.get("room")).substring("/itb/".length());
        String endpoint = "/itb/"+ room;
    	//String endpoint = "/service/xyz";
        
    	List<Object> posts = new ArrayList<Object>();     
    	if (post != null) {
    		posts.add(post);
    	}
        
    	ServerMessage.Mutable forward = _bayeux.newMessage();
    	forward.setChannel(endpoint);
    	forward.setId(msg.getId());
    	forward.setData(posts);
    	
    	// TODO: This may be shared across multiple threads and requests.....
    	if (client.isConnected()) {
    		client.deliver(_session, forward);
    	}
    }
    
    @Listener({"/service/echo"})
    protected void handleEchoMessage(ServerSession client, ServerMessage message)
    {
    	System.out.println("ClientSessionId= " + client + ", SeverSessionId= "+ _session + ", ThreadId= " + Thread.currentThread().getId() );
    	client.deliver(_session,"/service/echo", message, null);    	
    }

    @Configure ({"/service/itb", "/service/echo"})
    protected void configureMembers(ConfigurableServerChannel channel)
    {
    	//System.out.println("Configure channel" + channel.getId());
        channel.addAuthorizer(GrantAuthorizer.GRANT_PUBLISH);
        channel.setPersistent(true);           
    }

    /*
     * _members, is mapping of phrase to map of endpoint to client id.
     * Map<Phrase,Map<endpoint,clientId>>
     * Map<ford, Map<"/itb/demo", client_id>>
     */
    public void handleMembership(ServerSession client, ServerMessage message)
    {
    	System.out.println("handle service members");
        Map<String, Object> data = message.getDataAsMap();
        final String phrase = (String) data.get("phrase");
        Map<String, String> roomMembers= _members.get(phrase);
        if (roomMembers == null)
        {
            Map<String, String> new_room = new ConcurrentHashMap<String, String>();
            
            roomMembers = _members.putIfAbsent(phrase, new_room);
            if (roomMembers == null) roomMembers = new_room;
        }
        final Map<String, String> members = roomMembers;
        String room = ((String)data.get("room")).substring("/itb/".length());
        String endpoint = "/itb/"+ room;
        System.out.println("endpoint = "+ endpoint);
        if (endpoint != null) {
	        members.put(endpoint, client.getId());
	        client.addListener(new ServerSession.RemoveListener()
	        {
	            public void removed(ServerSession session, boolean timeout)
	            {
	                members.values().remove(session.getId());
	                //broadcastMembers(room,members.keySet());
	            }
	        });
        }

        //broadcastMembers(room,members.keySet());
    }

    private void broadcastMembers(String room, Set<String> members)
    {
        // Broadcast the new members list
        ClientSessionChannel channel = _session.getLocalSession().getChannel("/itb/"+room);
        channel.publish(members);
    }

    @Configure ("/service/privatechat")
    protected void configurePrivateChat(ConfigurableServerChannel channel)
    {
    	//System.out.println("Configure channel" + channel.getId());
        DataFilterMessageListener noMarkup = new DataFilterMessageListener(new NoMarkupFilter(),new BadWordFilter());
        channel.setPersistent(true);
        channel.addListener(noMarkup);
        channel.addAuthorizer(GrantAuthorizer.GRANT_PUBLISH);
    }

    @Listener("/service/privatechat")
    protected void privateChat(ServerSession client, ServerMessage message)
    {
        Map<String,Object> data = message.getDataAsMap();
        String room = ((String)data.get("room")).substring("/chat/".length());
        Map<String, String> membersMap = _members.get(room);
        if (membersMap==null)
        {
            Map<String,String>new_room=new ConcurrentHashMap<String, String>();
            membersMap=_members.putIfAbsent(room,new_room);
            if (membersMap==null)
                membersMap=new_room;
        }
        String[] peerNames = ((String)data.get("peer")).split(",");
        ArrayList<ServerSession> peers = new ArrayList<ServerSession>(peerNames.length);

        for (String peerName : peerNames)
        {
            String peerId = membersMap.get(peerName);
            if (peerId!=null)
            {
                ServerSession peer = _bayeux.getSession(peerId);
                if (peer!=null)
                    peers.add(peer);
            }
        }

        if (peers.size() > 0)
        {
            Map<String, Object> itb = new HashMap<String, Object>();
            String text=(String)data.get("itb");
            itb.put("chat", text);
            itb.put("user", data.get("user"));
            itb.put("scope", "private");
            ServerMessage.Mutable forward = _bayeux.newMessage();
            forward.setChannel("/itb/"+room);
            forward.setId(message.getId());
            forward.setData(itb);

            // test for lazy messages
            if (text.lastIndexOf("lazy")>0)
                forward.setLazy(true);

            for (ServerSession peer : peers)
                if (peer!=client)
                    peer.deliver(_session, forward);
            client.deliver(_session, forward);
        }
    }

    class BadWordFilter extends JSONDataFilter
    {
        @Override
        protected Object filterString(String string)
        {
            if (string.indexOf("dang")>=0)
                throw new DataFilter.Abort();
            return string;
        }
    }
    
    class StreamingClientSession {
    	public String channel;
    	public String sessionId;
    }
}
