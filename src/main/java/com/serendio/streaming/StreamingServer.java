/*
s * Copyright (c) 2010 the original author or authors.
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

package com.serendio.streaming;


import org.cometd.bayeux.server.BayeuxServer;
import org.cometd.java.annotation.AnnotationCometdServlet;
import org.cometd.server.CometdServlet;
import org.cometd.server.DefaultSecurityPolicy;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.bio.SocketConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

import com.serendio.REST.SearchServlet;



/* ------------------------------------------------------------ */
/**
 * Main class for cometd demo.
 */
public class StreamingServer
{
    /* ------------------------------------------------------------ */
    /**
     * @param args
     */
    public static void main(String[] args) throws Exception
    {
        int port = args.length==0?8080:Integer.parseInt(args[0]);

        String base= args.length==0?"/Users/jyadav/JITENDRA/workspace/StreamingAPI/src/main/webapp/jquery-examples":args[1];

        // Manually construct context to avoid hassles with webapp classloaders for now.
        Server server = new Server();
        QueuedThreadPool qtp = new QueuedThreadPool();
        qtp.setMinThreads(5);
        qtp.setMaxThreads(200);
        server.setThreadPool(qtp);

        SelectChannelConnector connector=new SelectChannelConnector();
        // SocketConnector connector=new SocketConnector();
        connector.setPort(port);
        connector.setMaxIdleTime(120000);
        connector.setLowResourcesMaxIdleTime(60000);
        connector.setLowResourcesConnections(20000);
        connector.setAcceptQueueSize(5000);
        server.addConnector(connector);
        //SocketConnector bconnector=new SocketConnector();
        //bconnector.setPort(port+1);
        //server.addConnector(bconnector);


        /*
        SslSelectChannelConnector ssl_connector=new SslSelectChannelConnector();
        ssl_connector.setPort(port-80+443);
        ssl_connector.setKeystore(base+"/examples/src/main/resources/keystore.jks");
        ssl_connector.setPassword("OBF:1vny1zlo1x8e1vnw1vn61x8g1zlu1vn4");
        ssl_connector.setKeyPassword("OBF:1u2u1wml1z7s1z7a1wnl1u2g");
        ssl_connector.setTruststore(base+"/examples/src/main/resources/keystore.jks");
        ssl_connector.setTrustPassword("OBF:1vny1zlo1x8e1vnw1vn61x8g1zlu1vn4");
        server.addConnector(ssl_connector);
        */

        //ContextHandlerCollection contexts = new ContextHandlerCollection();
        //server.setHandler(contexts);

        ResourceHandler resource_handler = new ResourceHandler();
        resource_handler.setDirectoriesListed(true);
        resource_handler.setWelcomeFiles(new String[]{ "index.html" });
        resource_handler.setResourceBase(base);
 
        HandlerList handlers = new HandlerList();
        handlers.setHandlers(new Handler[] { resource_handler, new DefaultHandler() });
        server.setHandler(handlers);
 
        // MovedContextHandler moved = new MovedContextHandler(contexts,"/","/cometd");
        // moved.setDiscardPathInfo(true);

        ServletContextHandler context = new ServletContextHandler(resource_handler,"/",ServletContextHandler.SESSIONS);

 //       ServletHolder dftServlet = context.addServlet(DefaultServlet.class, "/");
//        dftServlet.setInitOrder(1);

        // Cometd servlet
        CometdServlet cometdServlet = new AnnotationCometdServlet();
        ServletHolder comet = new ServletHolder(cometdServlet);
        context.addServlet(comet, "/cometd/*");
        comet.setInitParameter("timeout","20000");
        comet.setInitParameter("interval","100");
        comet.setInitParameter("maxInterval","10000");
        comet.setInitParameter("multiFrameInterval","5000");
        comet.setInitParameter("logLevel","1");
        comet.setInitParameter("services","com.serendio.streaming.StreamingService");
        
        // comet.setInitParameter("maxSessionsPerBrowser","4");
        comet.setInitParameter("transports","org.cometd.websocket.server.WebSocketTransport");
        comet.setInitOrder(1);
        
        // Search Servlet
        context.addServlet("com.serendio.REST.SearchServlet", "/search/*");
       
        server.start();

        BayeuxServer bayeux = cometdServlet.getBayeux();
        bayeux.setSecurityPolicy(new DefaultSecurityPolicy());

    }
}
