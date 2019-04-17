package br.icarocarmona.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount {
	public static void
	main(String[] args) {
		contarWordJava();
	}

	private static void contarWordJava() {
		SparkConf conf = new SparkConf().setAppName("AloMundoSpark").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		JavaRDD<String> input = context.textFile("C:\\Users\\icaro\\Downloads\\work\\java-spark\\log.txt");

		System.out.println(input.count());
		
		JavaRDD<String> linhasFiltradas = input.filter(l -> l.contains("Estefhani"));
		for (String string : linhasFiltradas.collect()) {
			System.out.println(string);
		}
		
		JavaRDD<String> palavras = input.flatMap(t -> Arrays.asList(t.split(" ")).iterator());

		
		JavaPairRDD<String, Integer> count = palavras.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
		JavaPairRDD<String, Integer> reduceCount = count.reduceByKey((v1, v2) -> v1 + v2);
		
		//reduceCount.saveAsTextFile("C:\\Temp\\output");
		
		context.close();
	}

}
