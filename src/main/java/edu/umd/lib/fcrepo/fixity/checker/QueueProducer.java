package edu.umd.lib.fcrepo.fixity.checker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.*;

public class QueueProducer implements Runnable {

	private LinkedBlockingQueue<String> queue;
	private File listFile;
	private String numberOfConsumers = System.getProperties().getProperty("request_threads", "5");
	private String batchWaitTime = System.getProperties().getProperty("batch_wait_time", "5000");

	
	public QueueProducer(LinkedBlockingQueue<String> queue, File listFile ) {
		this.queue = queue;
		this.listFile = listFile;
		
	}
	
	private void wait_for_queue() {
		try {
			Thread.sleep(Integer.parseInt(batchWaitTime.trim()));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public void run() {
		try( BufferedReader br = new BufferedReader(new FileReader(listFile))) {
		    
			// Start going through the list file and offer those to the queue. 
			// if the queue is full, wait a bit. 
			for(String line; (line = br.readLine()) != null; ) {
		    	while( !queue.offer(line) ) { 
		        	wait_for_queue();	
		        }
		    }
		    
		    // we've come to the end of the file.
		    // Start telling workers to stop. 
			int threadCount =  Integer.parseInt(numberOfConsumers.trim());
			for ( int t = 0; t < threadCount; t++ ) {
				while( !queue.offer("STOP") ) { 
		        	wait_for_queue();	
		        }
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
