package com.serendio.REST;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.json.JSONObject;
import org.json.XML;

import com.serendio.connector.hbase.SearchHBase;

public class SearchServlet implements Servlet{

	public void destroy() {
		// TODO Auto-generated method stub
		
	}

	public ServletConfig getServletConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getServletInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	public void init(ServletConfig arg0) throws ServletException {
		// TODO Auto-generated method stub
		
	}

	public void service(ServletRequest arg0, ServletResponse arg1)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		
		// Query and Category are mutually exclusive. Query has higher precedence
		String query = arg0.getParameter("query");
		String category = null;
		if (query == null) {
			category = arg0.getParameter("category");
		}
		String type = arg0.getParameter("type")!=null?arg0.getParameter("type"):"json";
		String validity = arg0.getParameter("validity")!=null?arg0.getParameter("validity"):"60";
		String limit = arg0.getParameter("limit")!=null?arg0.getParameter("limit"):"20";
		if(arg0.getParameter("DateRange") != null){
            SimpleRange aRange = null;
            String dateRange = arg0.getParameter("DateRange");
            System.out.println("Date Range is "+ dateRange);
            String min = "";
            String max = "";
            if(!dateRange.equalsIgnoreCase("No Min-No Max")){
                    if(dateRange.indexOf(":")!=-1){
                            min = dateRange.substring(0,dateRange.indexOf(":"));
                            max = dateRange.substring(dateRange.indexOf(":")+1,dateRange.length());
                            aRange = new SimpleRange("date_range",min,max);
                    }else if(dateRange.endsWith("+")){
                            min = dateRange.substring(0, dateRange.indexOf("+"));
                            aRange = new SimpleRange("date_range",min,null);
                    }else if(dateRange.endsWith("-")){
                            max = dateRange.substring(0, dateRange.indexOf("-"));
                            aRange = new SimpleRange("date_range",null,max);
                    }else if (dateRange.length() > 0){ // Exact match....
                            min = dateRange.substring(0, dateRange.length());
                            max = min;
                            aRange = new SimpleRange("date_range",min,max);
                    }
            }
		}
		// If DateRange and Validity both there, pick date range

    	List<Map<String, Object>> posts;
    	boolean hbase = true;
    	if (hbase) {
    		SearchHBase search = new SearchHBase("test");
    		// query, result set, type, validity
    		if (query != null) {
    			//TODO: check for negative values in validity and limit
    			System.out.println("query = " + query +", type = "+ type +", validity = "+ validity + ", limit = "+ limit);
    			posts = search.searchByExpression(query, Integer.valueOf(limit), type, Integer.valueOf(validity));
    		}else if (category != null) {
    			//TODO: check for negative values in validity and limit
    			System.out.println("category = " + category +", type = "+ type +", validity = "+ validity + ", limit = "+ limit);
    			posts = search.searchByCategory(category, Integer.valueOf(limit), type, Integer.valueOf(validity));
    		}else {
    			posts = new ArrayList<Map<String, Object>>();
    		}
    	}else{
    		posts = new ArrayList<Map<String,Object>>();
    		posts.add(dummyService(type));
    	}
    	
    	System.out.println("number of results " + posts.size());
    	//System.out.println(serialize(posts, type));
    	arg1.getOutputStream().print(serialize(posts,type));
    	
	}
	
	public Map<String,Object> dummyService(String type) {
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
        		if (!type.equalsIgnoreCase("xml")) {
        			try {
						post.put("Sentiment_Analysis", XML.toJSONObject(output));
					} catch (org.json.JSONException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else {
					post.put("Sentiment_Analysis", output);
					
				}
        		return post;
	}
	
	
	public String serialize(List<Map<String, Object>> posts, String format) {
		
		StringBuffer strBuffer = new StringBuffer();
		strBuffer.append('{');
		strBuffer.append("\"count\":");
		strBuffer.append(posts.size() +",\n");
		if (posts.size() > 0) {
			strBuffer.append("\"Results\": [");
			strBuffer.append("\n");
			for (Map<String, Object> post : posts) {
				strBuffer.append("{\n");
				Set<Entry<String,Object>> entry= post.entrySet();
				for (Entry<String,Object> e: entry) {
					String key = e.getKey();
					strBuffer.append("\""+key+"\": ");
					if (key.equalsIgnoreCase("img_url") || key.equalsIgnoreCase("tweet_url")||
							key.equalsIgnoreCase("tweet_data"))
					{
						String value = (String)e.getValue();
						strBuffer.append("\""+value+"\",\n");
					}else {// for XMl and JSON objects
						if (!format.equalsIgnoreCase("xml")) {
							//System.out.println(e.getValue());
							if (e.getValue().getClass().isInstance(JSONObject.class)) {
								JSONObject value = (JSONObject)e.getValue();
								strBuffer.append("\""+value+"\",\n");
							}else {
								strBuffer.append("\""+e.getValue()+"\",\n");
							}
							
						}else {
							String value = (String)e.getValue();
							strBuffer.append("\""+value+"\",\n");
						}
					}
					
					
							
				}
				strBuffer.append("},\n");
			}
			strBuffer.append("]\n");
		}
		strBuffer.append("}\n");
		return strBuffer.toString();
		
	}

}
