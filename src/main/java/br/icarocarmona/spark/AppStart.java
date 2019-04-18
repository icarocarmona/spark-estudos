package br.icarocarmona.spark;

import br.icarocarmona.spark.Entity.RunTask;
import br.icarocarmona.spark.Interface.IRunTask;


public class AppStart {

	private static IRunTask runTask;
	private final static String ARQUIVO_JUL95 = "C:\\Temp\\access_log_Jul95";
	private final static String ARQUIVO_AUG95 = "C:\\Temp\\access_log_Aug95";

	
	public static void main(String[] args) {
		//Ideal nessa parte o ideal é colcoar essa instancia na injeção do construtor
		runTask = new RunTask();
		runTask.Run(ARQUIVO_JUL95);
		runTask.Run(ARQUIVO_AUG95);
		
	}

}
