package edu.upenn.cis.nets2120.hw3.livy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import edu.upenn.cis.nets2120.config.Config;

public class ComputeRanksLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-54-221-57-240.compute-1.amazonaws.com:8998"))
				  .build();

		try {
			String jar = "target/nets2120-hw3-0.0.1-SNAPSHOT.jar";
			
		  System.out.printf("Uploading %s to the Spark context...\n", jar);
		  client.uploadJar(new File(jar)).get();
		  
		  String sourceFile = Config.FILE_PATH;

		  System.out.printf("Running SocialRankJob with %s as its input...\n", sourceFile);
		  List<MyPair<Integer,Double>> result = client.submit(new SocialRankJob(true, sourceFile)).get();
		  System.out.println("With backlinks: " + result);

		  // This is the part of code exclusively used for part 3.6
		  /* 
		  // using simple File and FileWriter to create a write to file
		  File my_bigger_file = new File("results2.txt");
		  FileWriter myBiggerWriter = new FileWriter(my_bigger_file);
		  // add each pair in the result to the output
		  result.stream().forEach(iter -> {
			try {
				myBiggerWriter.write(iter.toString());
			} catch (IOException e) {
				System.err.println(e);
			}
			try {
				// create a new line
				myBiggerWriter.write(System.lineSeparator());
			} catch (IOException e) {
				System.err.println(e);
			}
		  });
		  myBiggerWriter.close();
		  */
		  
		  // Part of the code exclusively used for part 3.5 (i.e., results1.txt)
		  List<MyPair<Integer,Double>> result_no_back = client.submit(new SocialRankJob(false, sourceFile)).get();
		  System.out.println("With backlinks: " + result_no_back);
		  // lst_true is the list of top 10 node in the execution with backlink
		  List<Integer> lst_true = new ArrayList<Integer>();
		  // lst_false is the list of top 10 nodes in the execution without backlink
		  List<Integer> lst_false = new ArrayList<Integer>();
		  // only pick the left one (the node id), since that's what we only want
		  result.stream().forEach(iter -> {
			  lst_true.add(iter.getLeft());
		  });
		  result_no_back.stream().forEach(iter -> {
			  lst_false.add(iter.getLeft());
		  });
		  // as its name suggest, lst_duplicate is used to store the common elements of lst_true and lst_false
		  List<Integer> lst_duplicate = new ArrayList<>(lst_true);
		  lst_duplicate.retainAll(lst_false);
		  lst_true.removeAll(lst_duplicate);
		  lst_false.removeAll(lst_duplicate);
		  File myFile = new File("results1.txt");
		  FileWriter myWriter = new FileWriter(myFile);
		  myWriter.write("Nodes in both lists: " + Arrays.toString(lst_duplicate.toArray()));
		  myWriter.write(System.lineSeparator());
		  myWriter.write("Nodes in computation with back-links: " + Arrays.toString(lst_true.toArray()));
		  myWriter.write(System.lineSeparator());
		  myWriter.write("Nodes in computation without back-links: " + Arrays.toString(lst_false.toArray()));
		  myWriter.close();
		  
		} finally {
		  client.stop(true);
		}
	}

}
