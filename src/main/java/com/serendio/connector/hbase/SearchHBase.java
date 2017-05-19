package com.serendio.connector.hbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;

public class SearchHBase {

	private static final String TABLE_NAME = "test";
	private static final String FAMILY_NAME = "info";
	private static final String QUALIFIER_NAME = "data";

	private HTable table;
	private String tableName = TABLE_NAME;
	private Properties fieldMappingProps = new Properties();

	public SearchHBase(String tableName) {
		if (tableName != null)
			this.tableName = tableName;

		Configuration hbaseConfig = HBaseConfiguration.create();
		// hbaseConfig.set("hbase.master", "78.46.77.101:8020");
		// hbaseConfig.set("hbase.zookeeper.quorum", "78.46.77.101");

		// try {
		// HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConfig);
		try {
			table = new HTable(hbaseConfig, tableName);
			InputStream is = Thread.currentThread().getContextClassLoader()
					.getResourceAsStream("field_mapping.properties");
			this.fieldMappingProps.load(is);
			is.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*
		 * } catch (MasterNotRunningException e) { // TODO Auto-generated catch
		 * block e.printStackTrace(); } catch (ZooKeeperConnectionException e) {
		 * // TODO Auto-generated catch block e.printStackTrace(); }
		 */
	}

	/*
	 * category to search for
	 * resultLimit, how many result required
	 * type, the format json and xml
	 * validity, how much old data required
	 */

	public List<Map<String, Object>> searchByCategory(String category,
			int resultLimit, String type, int validity) {

		SingleColumnValueFilter categoryFilter = new SingleColumnValueFilter(
				FAMILY_NAME.getBytes(), "categoryId".getBytes(),
				CompareOp.EQUAL, category.getBytes());
		// Set this to ignore the row with missing column
		categoryFilter.setFilterIfMissing(true);

		FilterList filterList = new FilterList();
		filterList.addFilter(categoryFilter);

		return searchByFilterList(filterList, resultLimit, type, validity);
	}

	/*
	 * exp is the search pattern 
	 * resultLimit, how many result required 
	 * type, is json or xml ageInSec, 
	 * search tweets as old as the given value
	 */
	public List<Map<String, Object>> searchByExpression(String exp,
			int resultLimit, String type, int validity) {

		// System.out.println("seraching hbase for expression \"" + exp + "\"");

		// Search in the info:data column for the expression
		RegexStringComparator comp = new RegexStringComparator(" " +exp + " "); 
		SingleColumnValueFilter dataFilter = new SingleColumnValueFilter(
				FAMILY_NAME.getBytes(), QUALIFIER_NAME.getBytes(),
				CompareOp.EQUAL, comp);
		// Set this to ignore the row with missing column
		dataFilter.setFilterIfMissing(true);

		FilterList filterList = new FilterList();
		filterList.addFilter(dataFilter);
		
		return searchByFilterList(filterList, resultLimit, type, validity);

	}

	/*
	 * Helper method to search by filter list
	 */
	public List<Map<String, Object>> searchByFilterList(FilterList fl,
			int resultLimit, String type, int validity) {
		ResultScanner rs = null;
		Scan scan = null;
		List<Map<String, Object>> posts = new ArrayList<Map<String, Object>>();

		try {
			scan = new Scan();

			scan.setFilter(fl);

			// Put the scan batch
			scan.setBatch(100);

			scan.setMaxVersions(1);

			// Add family to get all its columns
			scan.addFamily(FAMILY_NAME.getBytes());

			try {
				long max = 0;
				double total = 0;
				int count = 0;

				// System.out.println("validity is " + validity);
				if (validity > 0) {// do only when it is requested
					long maxStamp = System.currentTimeMillis();
					long minStamp = maxStamp - validity * 1000;
					scan.setTimeRange(minStamp, maxStamp);
					// System.out.println("Min : "+ minStamp + ", Max :" +
					// maxStamp);
				}

				rs = table.getScanner(scan);
				long start = System.currentTimeMillis();
				long min = start;

				// System.out.println(start +" " + min +" " + max + " "+ total
				// +" "+ count);
				for (Result r : rs) {

					long delta = System.currentTimeMillis() - start;
					if (delta > max) {
						max = delta;
					}
					if (delta < min) {
						min = delta;
					}
					total += delta;
					count++;
					// System.out.println(delta +" " + min +" " + max + " "+
					// total +" "+ count);

					String nullStr = "missing";
					Map<String, Object> post = new HashMap<String, Object>();
					Set<Entry<Object, Object>> fieldsEntries = fieldMappingProps
							.entrySet();
					for (Entry<Object, Object> e : fieldsEntries) {
						String field = (String) e.getKey();
						String column = (String) e.getValue();

						byte[] result = r.getValue(FAMILY_NAME.getBytes(),
								column.getBytes());
						String output = null;

						if (result != null) {
							output = new String(result);
						} else {
							output = nullStr;
						}

						// Look for XML/JSON object
						if (field.startsWith("--")) { // JSON or XML
							String key = field.substring(2);
							if (!type.equalsIgnoreCase("json")) { // in XML
																	// format.
								post.put(key, output);
							} else {// Convert the XML to JSON
								try {
									post.put(key, XML.toJSONObject(output));
								} catch (JSONException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}
							}
						} else { // for String object.
							post.put(field, output);							
						}

					}

					// System.out.println("Time for this call " +
					// (System.currentTimeMillis() - start));
					posts.add(post);

					start = System.currentTimeMillis();
					if ((resultLimit > 0) && (count == resultLimit)) {
						break;
					}
				}

				double avg = count == 0 ? 0 : total / count;
				min = count == 0 ? 0 : min;
				System.out.println("Search total time (msec) " + total
						+ ", avg " + avg + ", count " + count + ", min " + min
						+ ", max " + max);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} finally {
			if (rs != null)
				rs.close();
			// table.close();
		}

		return posts;
	}

	public List<Map<String, Object>> searchDB_old(String exp, int resultLimit,
			String type) {
		ResultScanner rs = null;
		Scan scan = null;
		List<Map<String, Object>> posts = new ArrayList<Map<String, Object>>();

		// System.out.println("seraching hbase for expression \"" + exp + "\"");
		try {
			scan = new Scan();
			// scanner =
			// table.getScanner(FAMILY_NAME.getBytes(),QUALIFIER_NAME.getBytes());

			// Search in the info:data column for the expression
			RegexStringComparator comp = new RegexStringComparator(exp); // any
																			// value
																			// that
																			// starts
																			// with
																			// 'my'
			SingleColumnValueFilter dataFilter = new SingleColumnValueFilter(
					FAMILY_NAME.getBytes(), QUALIFIER_NAME.getBytes(),
					CompareOp.EQUAL, comp);
			// Set this to ignore the row with missing column
			dataFilter.setFilterIfMissing(true);

			// Version = 1.1
			// Search in the info:ver column for given version
			SingleColumnValueFilter versionFilter = new SingleColumnValueFilter(
					FAMILY_NAME.getBytes(), "ver".getBytes(), CompareOp.EQUAL,
					"1.1".getBytes());
			// Set this to ignore the row with missing column
			versionFilter.setFilterIfMissing(true);

			FilterList filterList = new FilterList();
			filterList.addFilter(dataFilter);
			filterList.addFilter(versionFilter);

			scan.setFilter(filterList);

			// Put the scan batch
			scan.setBatch(100);

			scan.setMaxVersions(1);

			// Add the columns you need from this family
			// scan.addColumn(FAMILY_NAME.getBytes(), "ver".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(), QUALIFIER_NAME.getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(), "uri".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(),
					"author_profile_image_url".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(), "author_avatar".getBytes());

			scan.addColumn(FAMILY_NAME.getBytes(), "UserProfile".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(),
					"Sentiment_Analysis".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(),
					"activity_analysis".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(), "gender".getBytes());
			scan.addColumn(FAMILY_NAME.getBytes(), "ner".getBytes());

			try {
				long max = 0;
				double total = 0;
				int count = 0;

				rs = table.getScanner(scan);
				long start = System.currentTimeMillis();
				long min = start;

				// System.out.println(start +" " + min +" " + max + " "+ total
				// +" "+ count);
				for (Result r : rs) {

					long delta = System.currentTimeMillis() - start;
					if (delta > max) {
						max = delta;
					}
					if (delta < min) {
						min = delta;
					}
					total += delta;
					count++;
					// System.out.println(delta +" " + min +" " + max + " "+
					// total +" "+ count);
					Map<String, Object> post = new HashMap<String, Object>();
					byte[] result = r.getValue(FAMILY_NAME.getBytes(),
							QUALIFIER_NAME.getBytes());
					String output = "";
					// System.out.println(new String(result));
					post.put("tweet_data", new String(result));

					result = r.getValue(FAMILY_NAME.getBytes(),
							"uri".getBytes());
					// System.out.println(new String(result));
					post.put("tweet_url", new String(result));

					result = r.getValue(FAMILY_NAME.getBytes(),
							"author_avatar".getBytes());
					if (result == null)
						result = r.getValue(FAMILY_NAME.getBytes(),
								"author_profile_image_url".getBytes());
					if (result != null) {
						// System.out.println(new String(result));
						post.put("img_url", new String(result));
					} else
						post.put("img_url", new String("http://www.google.com"));

					// User profile.
					result = r.getValue(FAMILY_NAME.getBytes(),
							"UserProfile".getBytes());
					if (result == null) {
						output = new String("<UserProfile/>");
					} else {
						output = new String(result);
					}
					// System.out.println(output);
					if (!type.equalsIgnoreCase("json")) {
						post.put("UserProfile", output);
					} else {
						try {
							post.put("UserProfile", XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					// Sentiment Analysis
					result = r.getValue(FAMILY_NAME.getBytes(),
							"Sentiment_Analysis".getBytes());
					if (result == null) {
						output = new String("<Sentiment_Analysis/>");
					} else {
						output = new String(result);
					}

					// System.out.println(output);
					if (!type.equalsIgnoreCase("json")) {
						post.put("Sentiment_Analysis", output);
					} else {
						try {
							post.put("Sentiment_Analysis",
									XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					// Activity Analysis
					result = r.getValue(FAMILY_NAME.getBytes(),
							"activity_analysis".getBytes());
					if (result == null) {
						output = new String("<activity_analysis/>");
					} else {
						output = new String(result);
					}

					// System.out.println(output);
					if (!type.equalsIgnoreCase("json")) {
						post.put("activity_analysis", output);
					} else {
						try {
							post.put("activity_analysis",
									XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					// Gender
					result = r.getValue(FAMILY_NAME.getBytes(),
							"gender".getBytes());
					if (result == null) {
						output = new String("<gender/>");
					} else {
						output = new String(result);
					}

					// System.out.println(output);
					if (!type.equalsIgnoreCase("json")) {
						post.put("gender", output);
					} else {
						try {
							post.put("gender", XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					// NER
					result = r.getValue(FAMILY_NAME.getBytes(),
							"ner".getBytes());
					if (result == null) {
						output = new String("<ner/>");
					} else {
						output = new String(result);
					}

					// System.out.println(output);
					if (!type.equalsIgnoreCase("json")) {
						post.put("ner", output);
					} else {
						try {
							post.put("ner", XML.toJSONObject(output));
						} catch (JSONException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					/*
					 * result =
					 * r.getValue(FAMILY_NAME.getBytes(),"ver".getBytes() ); if
					 * (result !=null) { //System.out.println(new
					 * String(result)); post.put("ver", new String(result));
					 * }else post.put("ver", new String("null"));
					 */

					// System.out.println("Time for this call " +
					// (System.currentTimeMillis() - start));
					posts.add(post);

					start = System.currentTimeMillis();
					if ((resultLimit > 0) && (count == resultLimit)) {
						break;
					}
				}

				double avg = count == 0 ? 0 : total / count;
				min = count == 0 ? 0 : min;
				System.out.println("Search total time (msec) " + total
						+ ", avg " + avg + ", count " + count + ", min " + min
						+ ", max " + max);

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} finally {
			if (rs != null)
				rs.close();
			// table.close();
		}

		return posts;
	}

	public static void main(String[] args) {
		SearchHBase search = new SearchHBase("test");
		// search.searchDB("iphone");

	}

}
