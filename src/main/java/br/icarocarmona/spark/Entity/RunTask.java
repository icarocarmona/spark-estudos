package br.icarocarmona.spark.Entity;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import br.icarocarmona.spark.Interface.IRunTask;
import scala.Tuple2;

public class RunTask implements IRunTask {

	// delimitador para quebrar a linha em partes de acordo com o arquivo
	private final static String delimiter = "-.-\\w|\\[|\\]|\"| ";

	@Override
	public void Run(String PathFile) {
		// Configurações principais
		SparkConf conf = new SparkConf().setAppName("ProcessaLogNASA").setMaster("local");
		JavaSparkContext context = new JavaSparkContext(conf);
		try {

			// Carrega o arquivo de log local
			JavaRDD<String> data = context.textFile(PathFile);

			PrintTotalErros(data);
			PrintHostsUnicos(data);
			PrintTopCincoErros(data);
			PrintTotalBytes(data);
			PrintQuantidadeErrosDia(data);

		} finally {
			context.close();
		}

	}

	private void PrintQuantidadeErrosDia(JavaRDD<String> data) {

		//Filtra apenas os erros 404
		JavaRDD<String> erros = FiltraErros(data);

		//cria uma nova coleção com a data 
		JavaPairRDD<String, Integer> filtered = erros
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[4].split(":")[0], 1));

		//Agrupa as datas e soma
		JavaPairRDD<String, Integer> errosPorDia = filtered.reduceByKey((x, y) -> x + y);

		System.out.println("---- Quantidade de erros por dia ----");
		errosPorDia.foreach((f) -> System.out.println(f));
	}

	private void PrintTotalBytes(JavaRDD<String> data) {
		//cria uma coleção com apenas o valor de Bytes
		JavaRDD<Integer> dataFiltered = data.flatMap(t -> {
			try {
				return Arrays.asList(Integer.parseInt(t.split(delimiter)[13])).iterator();

			} catch (Exception e) {
				return Arrays.asList(0).iterator();
			}
		});

		//Soma total de bytes
		long count = dataFiltered.reduce((a, b) -> a + b);
		System.out.println("-----  Total bytes : " + count);

	}

	private void PrintTopCincoErros(JavaRDD<String> data) {

		JavaRDD<String> erros = FiltraErros(data);

		//monta par com o host
		JavaPairRDD<String, Integer> pair = erros
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[0], 1));
		List<Tuple2<String, Integer>> group = pair.reduceByKey((x, y) -> x + y).sortByKey(true).take(5);

		System.out.println("---- Os 5 URLs que mais causaram erro 404 ----");
		
		//Imprime os hosts com mais erros
		for (Tuple2<String, Integer> host : group) {
			System.out.println(host._1);
		}

	}

	//Filtra apenas os erros 404 dos dados
	private static JavaRDD<String> FiltraErros(JavaRDD<String> data) {
		return data.filter(f -> f.contains("404"));
	}

	private static void PrintTotalErros(JavaRDD<String> data) {
		JavaRDD<String> totalErros = FiltraErros(data);
		System.out.println("---- Total de erros 404 é: " + totalErros.count());
	}

	private static void PrintHostsUnicos(JavaRDD<String> data) {
		
		//cria um novo para com o Host 
		JavaPairRDD<String, Integer> pair = data
				.mapToPair(m -> new Tuple2<String, Integer>(m.split(delimiter)[0], 1));
		
		//Agrupa os dados
		JavaPairRDD<String, Integer> group = pair.reduceByKey((x, y) -> x + y);
		
		//filtra apenas os hosts que tem unico acesso
		JavaPairRDD<String, Integer> filter = group.filter(f -> f._2.equals(1));

		System.out.println("---- Total de hosts unicos :" + filter.count());
	}

}
