package edu.umd.lib.fcrepo.fixity.checker;

import java.io.File;
import java.util.concurrent.LinkedBlockingQueue;

import edu.umd.lib.fcrepo.fixity.checker.QueueConsumer;
import edu.umd.lib.fcrepo.fixity.checker.QueueProducer;




public class FixityBatchDispatcher {

	private LinkedBlockingQueue<String> queue;
	private File listFile;
	private String numberOfConsumers = System.getProperties().getProperty("request_threads", "5");

	
	public FixityBatchDispatcher(File listFile) {
		this.queue =  new LinkedBlockingQueue<String>(Integer.parseInt(numberOfConsumers.trim()));
		this.listFile = listFile;
	}
	
	public void run() { 
		
		Thread producer = new Thread( new QueueProducer(queue, listFile));
		producer.start();
	
		int threadCount =  Integer.parseInt(numberOfConsumers.trim());		
		Thread consumers[] = new Thread[threadCount];
		for ( int t = 0; t < threadCount; t++ ) {
			consumers[t] = new Thread(new QueueConsumer(queue));
			consumers[t].start();
		}
			
	}
}
