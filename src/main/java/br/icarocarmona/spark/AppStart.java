package br.icarocarmona.spark;

import br.icarocarmona.spark.Entity.RunTask;
import br.icarocarmona.spark.Interface.IRunTask;

public class AppStart {

	private static IRunTask runTask;

	public static void main(String[] args) {
		//Ideal nessa parte o ideal � colcoar essa instancia na inje��o do construtor
		runTask = new RunTask();
		runTask.Run();
	}

}
