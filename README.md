# Spark-estudos
Projeto tem como objetivo o apreendizado do mecanismo Spark.

### Conf do micro
- Sistema: Win 10
- IDE: Eclipse 2019-03
- JRE 1.8.0

#Configurações necessarias para o Windows 10
- Baixar hadoop-3.0.0 do link https://github.com/steveloughran/winutils
- Colocar essa pasta em um local de preferencia
	- No meu micro ficou da seguinte forma "C:/hadoop-3.0.0/bin/winutils.exe"
- Agora no Eclipse com o projeto aberto configure acessando > Run / Run Configurations..
- Agora na aba Main clique em Search dentro de Main Class e selecione a class Programa.java

- Na mesma tela acesse a aba Environment > Clique em New... > Em Name adicione: HADOOP_HOME e em Value adicione o caminho do winutil, no meu ficou o seguinte exemplo  C:\winutil

- `Obs.: Não deve colocar a pasta bin do Value.`

### Dependencias utilizadas
 - spark-core_2.12, versão 2.4.0 (Nessario para rodar o spark)
 - paranamer, versão 2.8 (necessário por causa da utilização do Java8 e se utilizar o metodo parallelize da class JavaSparkContext)