package edu.upenn.cis.nets2120.config;

public class Config {

	/**
	 * The path to the space-delimited social network data
	 */
	public static String FILE_PATH = "/home/nets2120/G10/newsSparkGraphSmall.txt";
	
	public static String LOCAL_SPARK = "local[*]";

	/**
	 * How many RDD partitions to use?
	 */
	public static int PARTITIONS = 5;
}