package edu.umd.lib.fcrepo.fixity.checker;

import java.io.IOException;
import java.util.ArrayList;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.validator.routines.UrlValidator;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.ParseException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.client.methods.HttpPost;                                                                                                                                           
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;                                                                                                                          
import org.apache.http.impl.nio.client.HttpAsyncClients;                                                                                                                                                                                                                                                                                  
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils; 


public class QueueConsumer implements Runnable {

	private LinkedBlockingQueue<String> queue;
	private CloseableHttpAsyncClient client;
	private UrlValidator urlValidator = new UrlValidator(UrlValidator.ALLOW_LOCAL_URLS);
	private String endpoint = System.getProperties().getProperty("fixity_endpoint","http://localhost:9080/reindexing/" );


	
	public QueueConsumer(LinkedBlockingQueue<String> queue ) {
	    this.queue = queue;	    
		ArrayList<BasicHeader> headers = new ArrayList<BasicHeader>();
		headers.add( new BasicHeader("Content-Type", "application/json"));
		this.client = HttpAsyncClients.custom().setDefaultHeaders(headers).build();
	}
	
	public void run() {
		client.start();
		try { 
			while(true) {
				String url = queue.take();
				
				if ( url == "STOP" ) {
					// System.out.println("STOP received");
					break;
				}
				
				consume(url);
			}
			
		} catch (InterruptedException ie) {
			ie.printStackTrace();	
		}
		
		try {
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void consume(String url) {
		// If the URL has /rest/ it in, its a FCREPO uri, not a reindexing URI.
		// We can just do some fixes there.
		if ( url.contains("/rest/")) { 
			url = endpoint + url.substring(url.lastIndexOf("/rest/")+6);
		}
		

		// spit it out if it's not valid. 
		if ( !urlValidator.isValid(url)) { 
			System.out.println("Error: Not a valid URL = " + url);
			return; 
		}
		
		HttpPost request = new HttpPost(url);
		StringEntity stringEntity = new StringEntity("[\"activemq:queue:fedorafixity\"]", ContentType.APPLICATION_JSON);
		request.setEntity(stringEntity);
		
		Future<?> future = client.execute(request, null);
		
		try {
			HttpResponse response = (HttpResponse) future.get();
			Integer statusCode = response.getStatusLine().getStatusCode();
			Boolean success = ( statusCode >= 200 ) && ( statusCode < 400 );
			if (success) { 
				System.out.println(getMessage(response));
			} else { 
				System.out.println("Failure with: " + url);
			}
		} catch (InterruptedException | ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String getMessage(HttpResponse response) {
		HttpEntity entity = response.getEntity();
		String responseString = "";
		try {
			responseString = EntityUtils.toString(entity, "UTF-8");
		} catch (ParseException e) {
			e.printStackTrace();

		} catch (IOException e) {
			e.printStackTrace();
		}
		return responseString;
	}
}
