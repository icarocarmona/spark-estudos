package br.icarocarmona.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTeste {

	public static void main(String[] args) {
		
	//	System.setProperty("hadoop.home.dir", "C:/winutil/");
        
		SparkConf conf = new SparkConf().setAppName("AloMundoSpark").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);

		List<String> lista = Arrays.asList(new String[] { "Icaro", "Carmona", "Almeida", "Estefhani", "Thanos" });

		JavaRDD<String> rdd = context.parallelize(lista);
		rdd.foreach((nome) -> System.out.println(nome));
		
		
		
		/*
		 * SparkConf conf = new
		 * SparkConf().setMaster("local").setAppName("BusProcessor"); JavaSparkContext
		 * ctx = new JavaSparkContext(conf);
		 * 
		 * //carrega os dados dos ônibus de sp JavaRDD<String> linhas =
		 * ctx.textFile("c:/Users/icaro/Downloads/work/java-spark/log.txt"); long
		 * numeroLinhas = linhas.count();
		 * 
		 * // escreve o número de ônibus que existem no arquivo
		 * System.out.println(numeroLinhas);
		 * 
		 * ctx.close();
		 */
         

	}
}
