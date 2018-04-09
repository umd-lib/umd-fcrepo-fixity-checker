package edu.umd.lib.fcrepo.fixity.checker;

import java.io.File;

public class FixityChecker {

	public static void main(String[] args) {
		if ( args.length == 0) {
			System.out.println("Missing arguement.");
			System.out.println("You must pass in a list file of URIs to process");
			System.out.println("E.g. java -jar umd-fixity-batch.jar mylist.txt");
			return;
		}
		
		File f = new File(args[0]);
		if ( !f.exists() || f.isDirectory() ) { 
			System.out.println("Cannot find file: " + f.toString());
			System.out.println("Please check you file path and try again.");
			return;
		}
		
		FixityBatchDispatcher dispatcher = new FixityBatchDispatcher(f);
		dispatcher.run();
		
				
	}

}
