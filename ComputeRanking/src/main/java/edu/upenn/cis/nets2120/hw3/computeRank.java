package edu.upenn.cis.nets2120.hw3;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.io.PrintWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.SparkConnector;

public class computeRank {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(computeRank.class);

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;
	
	JavaSparkContext context;
	
	public computeRank() {
		System.setProperty("file.encoding", "UTF-8");
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void initialize() throws IOException, InterruptedException {
		logger.info("Connecting to Spark...");

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();
		
		logger.debug("Connected!");
	}
	
	/**
	 * Fetch the social network from the S3 path, and create a (followed, follower) edge graph
	 * 
	 * @param filePath
	 * @return JavaPairRDD: (followed: int, follower: int)
	 */
	JavaRDD<String[]> getSocialNetwork(String filePath) {
		// Load the file filePath into an RDD (take care to handle both spaces and tab characters as separators)
		JavaRDD<String[]> file = context.textFile(filePath, Config.PARTITIONS)
				.map(line -> {
					String[] words = line.toString().split("###");
					if (words.length!=4) {
						/*
						String something = "1^2^2^3";
						String[] wds = something.toString().split("^");
						for (int i = 0; i < wds.length; i++) {
							logger.info("the "+String.valueOf(i)+"th expression is: "+wds[i]);
						}*/
						logger.info(line+" not having all four fields");
						for (int i = 0; i < words.length; i++) {
							logger.info("the "+String.valueOf(i)+"th expression is: "+words[i]);
						}
					}
					return line.toString().split("###");
				});
		return file;
	}
	/**
	 * Main functionality in the program: read and process the social network
	 * 
	 * @throws IOException File read, network, and other errors
	 * @throws InterruptedException User presses Ctrl-C
	 */
	public void run(String[] args) throws IOException, InterruptedException {
		logger.info("Running");
		double dmax = 30;
		double imax = 15;
		boolean debug_mode = false;
		if (args.length>0) {
			// the first argument. dmax is the threshold for change in rank
			dmax = Double.valueOf(args[0]);
		}
		if (args.length>1) {
			// the second argument. imax is the maximum number of rounds
			imax = Double.valueOf(args[1]);
		}
		if (args.length>2) {
			// if args.length>2 then the third element exist. We enter the debug mode.
			debug_mode = true;
		}
		// Load the social network
		// followed, follower
		JavaRDD<String[]> network = getSocialNetwork(Config.FILE_PATH);
		JavaRDD<String> users = network.filter(it -> Integer.valueOf(it[2])==0)
				.map(iter -> iter[0]).distinct();
		JavaPairRDD<String,Tuple2<String,Double>> edgeTransferRDD_label = users
				.mapToPair(iter -> new Tuple2<String, Tuple2<String, Double>>("3"+iter.substring(1),
						new Tuple2<String,Double>(iter,1)));
		network = network.distinct();
		JavaPairRDD<String, String> user_cat = network.filter(it -> Integer.valueOf(it[2])==0 && Integer.valueOf(it[3])==1)
				.mapToPair(itm -> new Tuple2<String,String>(itm[0],itm[1]));
		JavaPairRDD<String, String> from_cat = network.filter(it -> Integer.valueOf(it[2])==1)
				.mapToPair(itm -> new Tuple2<String,String>(itm[0],itm[1]));
		JavaPairRDD<String, String> from_art = network.filter(it -> Integer.valueOf(it[2])==2)
				.mapToPair(itm -> new Tuple2<String,String>(itm[0],itm[1]));
		JavaPairRDD<String, String> user_art = network.filter(it -> Integer.valueOf(it[2])==0 && Integer.valueOf(it[3])==2)
				.mapToPair(itm -> new Tuple2<String,String>(itm[0],itm[1]));
		JavaPairRDD<String, String> user_user = network.filter(it -> Integer.valueOf(it[2])==0 && Integer.valueOf(it[3])==0)
				.mapToPair(itm -> new Tuple2<String,String>(itm[0],itm[1]));
		// calculate the nodeTransferRDD
		JavaPairRDD<String,Double> nTRDD_UC = user_cat
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,0.3/iter._2));
		JavaPairRDD<String,Double> nTRDD_UU = user_user
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,0.3/iter._2));
		JavaPairRDD<String,Double> nTRDD_UA = user_art
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,0.4/iter._2));
		JavaPairRDD<String,Double> nTRDD_C = from_cat
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0/iter._2));
		JavaPairRDD<String,Double> nTRDD_A = from_art
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0))
				.reduceByKey((a,b) -> a+b)
				.mapToPair(iter -> new Tuple2<String,Double>(iter._1,1.0/iter._2));
		JavaPairRDD<String,Double> nodeTransferRDD = context.union(nTRDD_UC,nTRDD_UU,nTRDD_UA,nTRDD_C,nTRDD_A);
		logger.info("nodeTransferRDD contains "+String.valueOf(nodeTransferRDD.count())+" entries");
		JavaPairRDD<String,Tuple2<String,Double>> edgeTransferRDD = network.mapToPair(
				iter -> new Tuple2<String,String>(iter[0],iter[1])).join(nodeTransferRDD);
		edgeTransferRDD = context.union(nodeTransferRDD,edgeTransferRDD_label);
		double d = 0.15;
		// initialize all pageRank as 1.0
		JavaPairRDD<String,Double> pageRankRDD = network.mapToPair(
				iter -> new Tuple2<String,Double>(iter[0],1.0));
		pageRankRDD = pageRankRDD.distinct();
		for (int i = 0; i < imax; i++) {
			JavaPairRDD<String,Double> propagateRDD = edgeTransferRDD
					.join(pageRankRDD)
					.mapToPair(
							iter -> new Tuple2<String,Double>(
									iter._2._1._1,iter._2._2 * iter._2._1._2));
			// calculate the new page rank using the equation given in handout
			JavaPairRDD<String,Double> pageRankRDD2 = propagateRDD
					.reduceByKey((a,b) -> a+b)
					.mapToPair(iter -> new Tuple2<String,Double>(iter._1,d+(1-d)*iter._2));
			if (debug_mode) {
				// if in debug mode, then each round output the page rank
				System.out.println("round "+String.valueOf(i));
				pageRankRDD2.collect().stream().forEach(iter -> {
					System.out.println(String.valueOf(iter._1)+" "+String.valueOf(iter._2));
				});
			}
			// calculate the difference for the page rank of each node in this round
			JavaRDD<Double> diff = context.union(pageRankRDD2,pageRankRDD)
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
		logger.info("*** Finished social network ranking! ***");
		// sort the pageRankRDD by the value (i.e., page rank)
		pageRankRDD = pageRankRDD.mapToPair(iter -> new Tuple2<>(iter._2,iter._1))
				.sortByKey(false)
				.mapToPair(iter -> new Tuple2<>(iter._2,iter._1));
		JavaPairRDD<String,Double> articleRankRDD = pageRankRDD.filter(iter -> Integer.parseInt(String.valueOf(iter._1.charAt(0)))==2);
		PrintWriter writer = new PrintWriter("articleRanks.txt", "UTF-8");
		articleRankRDD.map(iter -> {
			writer.println(iter._1+"###"+String.valueOf(iter._2));
			return iter;
		});
	}


	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		if (spark != null)
			spark.close();
	}
	
	

	public static void main(String[] args) {
		final computeRank cr = new computeRank();

		try {
			cr.initialize();
			cr.run(args);
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			cr.shutdown();
		}
	}

}
