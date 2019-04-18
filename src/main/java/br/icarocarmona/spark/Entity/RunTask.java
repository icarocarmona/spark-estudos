package br.icarocarmona.spark.Entity;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import br.icarocarmona.spark.Interface.IRunTask;
import scala.Tuple2;

public class RunTask implements IRunTask {

	private final static String ARQUIVO_JUL952 = "C:\\Temp\\access_log_Jul952";

	// delimitador para quebrar a linha em partes
	private final static String delimiter = "-.-\\w|\\[|\\]|\"| ";

	@Override
	public void Run() {
		// Configurações principais
		SparkConf conf = new SparkConf().setAppName("ProcessaLogNASA").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		try {

			// Carrega o arquivo de log local
			JavaRDD<String> data = context.textFile(ARQUIVO_JUL952);

			
			
			// TotalErros(data);
			// HostsUnicos(data);
			// PrintTopCincoErros(data);
			//PrintTotalBytes(data);
			PrintQuantidadeErrosDia(data);
		} finally {
			context.close();
		}

	}

	private void PrintQuantidadeErrosDia(JavaRDD<String> data) {
		//data.flatMap(f -> )
	}

	private void PrintTotalBytes(JavaRDD<String> data) {
		JavaRDD<Integer> dataFiltered = data.flatMap(t -> {
			try {
				return Arrays.asList(Integer.parseInt(t.split(delimiter)[13])).iterator();

			} catch (Exception e) {
				return Arrays.asList(0).iterator();
			}
		});

		int count = dataFiltered.reduce((a, b) -> a + b);
		System.out.println("Total filtrado : " + count);
	}

	private void PrintTopCincoErros(JavaRDD<String> data) {

		JavaRDD<String> erros = data.filter(f -> f.contains("404"));

		JavaPairRDD<String, Integer> agrupaLog = erros
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[0], 1));
		List<Tuple2<String, Integer>> numeroErros = agrupaLog.reduceByKey((x, y) -> x + y).sortByKey(true).take(5);

		for (Tuple2<String, Integer> tuple2 : numeroErros) {
			System.out.println(tuple2._1);
		}

	}

	private static long TotalErros(JavaRDD<String> data) {
		JavaRDD<String> totalErros = data.filter(f -> f.contains("404"));
		System.out.println("Total de erros é: " + totalErros.count());
		return totalErros.count();
	}

	private static long HostsUnicos(JavaRDD<String> data) {
		JavaPairRDD<String, Integer> agrupaLog = data
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[0], 1));
		JavaPairRDD<String, Integer> numeroHosts = agrupaLog.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Integer> filter = numeroHosts.filter(f -> f._2.equals(1));

		System.out.println("Total de hosts unicos :" + filter.count());

		return filter.count();
	}

}
