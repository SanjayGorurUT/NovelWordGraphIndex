import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import java.io.*;

public final class WordGraph {

    /*
    * The main function needs to create a word graph of the text files provided in arg[0]
    * The output of the word graph should be written to arg[1]
    */
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
        .builder()
        .appName("JavaSparkPi")
        .getOrCreate();
  
      JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

      JavaRDD<String> rdd = jsc.textFile(args[0] + "/*", 8);
	  JavaRDD<String> allWords = rdd.flatMap(line -> {
			List<String> output = new ArrayList<>();
			String[] words = line.split("(?![\\p{Punct}\\s])[\\W]+");
			output = Arrays.asList(words);
			return output.iterator();
		});
	
	  JavaRDD<String> cleanedWords = allWords.map(w -> w.replaceAll("[^a-zA-Z0-9]+", " ").toLowerCase()).filter(s -> !s.isEmpty());

	  JavaPairRDD<String, Tuple2<String, Integer>> allPairs = cleanedWords.flatMapToPair(line -> {
		String[] words = line.split(" ");
		List<Tuple2<String, Tuple2<String, Integer>>> tuples = new ArrayList<>();
		for (int i = 0; i < words.length - 1; i++) {
			if(!words[i].isEmpty() && !words[i + 1].isEmpty()) {
				tuples.add(new Tuple2<>(words[i], new Tuple2<>(words[i + 1], 1)));
			}
		}
		return tuples.iterator();
	  });

	  JavaPairRDD<Tuple2<String, String>, Integer> counts = allPairs
        .mapToPair(pair -> new Tuple2<>(new Tuple2<>(pair._1, pair._2._1), pair._2._2))
        .reduceByKey((count1, count2) -> count1 + count2);

	  JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupedCounts = counts
    	.mapToPair(pair -> new Tuple2<>(pair._1()._1(), new Tuple2<>(pair._1()._2(), pair._2())))
    	.groupByKey();

	  JavaRDD<String> fileOutput = groupedCounts.map(entry -> {
		int amount = 0;
		double sum = 0;
		for(Tuple2<String, Integer> tuple : entry._2()){
            sum += tuple._2();
			amount++;
        }
		String output = entry._1() + " " + amount + "\n";
        for(Tuple2<String, Integer> tuple : entry._2()){
			if(tuple._2() != sum)
			{
				double weight = ((double) (tuple._2()))/(sum);
				double factor = Math.pow(10, 3);
				weight = Math.round(weight * factor) / factor;
				output += "<" + tuple._1() + ", " + weight + ">\n";
			}
			else
			{
				output += "<" + tuple._1() + ", 1>\n";
			}
        }
		return output;
      });

	  try {
		File dir = new File(new File(args[1]).getParent());
		if(!dir.exists()) {
			dir.mkdirs();
		}
		File file = new File(args[1]);
		if(!file.exists()) {
			file.createNewFile();
		}
		FileWriter writer = new FileWriter(file);
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
        bufferedWriter.write(fileOutput.reduce((s1, s2) -> s1 + s2));
        bufferedWriter.close();
	  }
	  catch(Exception e) {
		System.out.println("Issue writing to file");
		e.printStackTrace();
	  }

      spark.stop();
    }
}