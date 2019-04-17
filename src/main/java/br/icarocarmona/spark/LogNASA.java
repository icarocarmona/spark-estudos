package br.icarocarmona.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class LogNASA {

	public static void main(String[] args) {

		ProcessaLog();
	}

	private static void ProcessaLog() {

		// String[] teste = texto.split("-.-\\w|\\[|\\]|\"| ");

		SparkConf conf = new SparkConf().setAppName("ProcessaLogNASA").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		String delimiter = "-.-\\w|\\[|\\]|\"| ";
		// Carrega log da NASA
		JavaRDD<String> data = context.textFile("C:\\Temp\\access_log_Jul952");

		JavaRDD<String> totalErros = data.filter(f -> f.contains("404"));
		System.out.println("Total de erros é: " + totalErros.count());

		
		
		JavaPairRDD<String, Integer> agrupaLog = data
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[0], 1));
		JavaPairRDD<String, Integer> numeroHosts = agrupaLog.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Integer> filter = numeroHosts.filter(f -> f._2.equals(1));
		System.out.println("Total de hosts unicos :" + filter.count());

		
		
//		JavaRDD<String> logs = data.flatMap(f -> Arrays.asList(f.split(delimiter)).iterator());
//		
//		for (String string : logs.collect()) {
//			System.out.println();
//		}
//		
//		JavaPairRDD<String, Integer> count = logs.mapToPair(t -> new Tuple2<String, Integer>(t, 1));
//		System.out.println(count.count());
//		
//		
//		JavaPairRDD<String, Integer> reduceCount = count.reduceByKey((v1, v2) -> v1 + v2);
//		System.out.println(reduceCount.count());
//		
//		
//		reduceCount.saveAsTextFile("C:\\Temp\\output");

		context.close();
	}
}
