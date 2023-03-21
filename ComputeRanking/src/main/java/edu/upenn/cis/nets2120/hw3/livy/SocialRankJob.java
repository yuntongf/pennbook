package edu.upenn.cis.nets2120.hw3.livy;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class SocialRankJob implements Job<List<MyPair<Integer,Double>>> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;

	private boolean useBacklinks;

	private String source;
	

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 * @throws DynamoDbException 
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to Spark...");
		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		System.out.println("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaPairRDD<Integer,Integer> getSocialNetwork(String filePath) {
		// Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> line.toString().contains(" ")?
						line.toString().split(" "):line.toString().split("\t"));
		System.out.println("finished reading");
		return file.mapToPair(itm -> new Tuple2<Integer,Integer>(Integer.valueOf(itm[0]),
				Integer.valueOf(itm[1])));
	}
	
	private JavaRDD<Integer> getSinks(JavaPairRDD<Integer,Integer> network) {
		// Find the sinks in the provided graph
		JavaRDD<Integer> fld = network.map(iter -> iter._2()).distinct();
		JavaRDD<Integer> flr = network.map(iter -> iter._1()).distinct();
		// the sinks by definition are the followed nodes that do not follow anyone 
		return fld.subtract(flr);
	}

	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws DynamoDbException DynamoDB is unhappy with something
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public List<MyPair<Integer,Double>> run() throws IOException, InterruptedException {
		System.out.println("Running");
		double dmax = 30;
		double imax = 25;
		// Load the social network
		// followed, follower
		JavaPairRDD<Integer, Integer> network = getSocialNetwork(source);
		System.out.println("network constructed");
		
		network = network.distinct();
		
		long num_edge = network.count();
		JavaRDD<Integer> fld = network.map(iter -> iter._2()).distinct();
		JavaRDD<Integer> flr = network.map(iter -> iter._1()).distinct();
		JavaRDD<Integer> nodes = fld.union(flr);
		long num_nodes = nodes.distinct().count();
		System.out.println("This graph contains "+String.valueOf(num_nodes)+" nodes and "+
				String.valueOf(num_edge)+" edges");
		
		
		// find the sinks
		JavaRDD<Integer> sinks = getSinks(network);

		// if useBacklinks is true then we add backlinks, following same procedure as in ComputeRanks.java
		
		if (useBacklinks) {
			JavaPairRDD<Integer,Integer> reversed_network = network.mapToPair(
					iter -> new Tuple2<>(iter._2,iter._1));
			JavaPairRDD<Integer,Boolean> has_sink = sinks.mapToPair(iter -> new Tuple2<Integer,Boolean>(iter,true));
			JavaPairRDD<Integer,Tuple2<Integer,Boolean>> contains = reversed_network.join(has_sink);
			JavaPairRDD<Integer,Integer> pair_contain_sink = contains.mapToPair(iter -> new Tuple2<Integer,Integer>(
					iter._1,iter._2._1));
			pair_contain_sink = pair_contain_sink.distinct();
			network = network.union(pair_contain_sink);
			System.out.println("Added "+String.valueOf(network.count()-num_edge)+" backlinks");
		}
		// this part is the same as in ComputeRanks, which is basically the same as shown in lab videos
		JavaPairRDD<Integer,Double> nodeTransferRDD = network
				.mapToPair(iter -> new Tuple2<Integer,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<Integer,Double>(iter._1,1.0/iter._2));
		JavaPairRDD<Integer,Tuple2<Integer,Double>> edgeTransferRDD = network.join(nodeTransferRDD);
		double d = 0.15;
		JavaPairRDD<Integer,Double> pageRankRDD = network.mapToPair(
				iter -> new Tuple2<Integer,Double>(iter._1,1.0));
		pageRankRDD = pageRankRDD.distinct();
		for (int i = 0; i < imax; i++) {
			System.out.println("round "+String.valueOf(i));
			JavaPairRDD<Integer,Double> propagateRDD = edgeTransferRDD
					.join(pageRankRDD)
					.mapToPair(
							iter -> new Tuple2<Integer,Double>(
									iter._2._1._1,iter._2._2 * iter._2._1._2));
			JavaPairRDD<Integer,Double> pageRankRDD2 = propagateRDD
					.reduceByKey((a,b) -> a+b)
					.mapToPair(iter -> new Tuple2<Integer,Double>(iter._1,d+(1-d)*iter._2));
			// calculate the difference for the page rank of each node in this round
			JavaRDD<Double> diff = pageRankRDD2.union(pageRankRDD)
					.reduceByKey((a,b) -> Math.abs(a-b))
					.map(iter -> iter._2);
			// calculate the maximum change in page rank
			double max_diff = diff.reduce((a,b) -> Math.max(a,b));
			pageRankRDD= pageRankRDD2;
			if (max_diff <= dmax) {
				// if maximum change in page rank smaller than threshold, break
				break;
			}
		}
		System.out.println("*** Finished social network ranking! ***");
		// sort the pageRankRDD by the value (i.e., page rank)
		pageRankRDD = pageRankRDD.mapToPair(iter -> new Tuple2<>(iter._2,iter._1))
				.sortByKey(false)
				.mapToPair(iter -> new Tuple2<>(iter._2,iter._1));
		// take the top 10
		List<Tuple2<Integer,Double>> top_ten = pageRankRDD.take(10);
		// convert to List of MyPair
		List<MyPair<Integer,Double>> answer = new ArrayList<MyPair<Integer,Double>>();
		top_ten.stream().forEach(iter -> {
			answer.add(new MyPair(iter._1,iter._2));
		});
		return answer;
	}

	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");
	}
	
	public SocialRankJob(boolean useBacklinks, String source) {
		System.setProperty("file.encoding", "UTF-8");
		
		this.useBacklinks = useBacklinks;
		this.source = source;
	}

	@Override
	public List<MyPair<Integer,Double>> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
