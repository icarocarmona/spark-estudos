# Spark-estudos
Projeto tem como objetivo o aprendizado do mecanismo Spark.

### Log
O arquivo utilizado para o estudo foram os seguintes:

- ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
- ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

Obs.: Baixo os arquivos antes de tudo pois são grandes

### Conf da aplicação
 - Adicione os arquivos de Log no seguinte caminho "C:\Temp" e descompacte o mesmo.

### Conf do micro
- Sistema: Win 10
- IDE: Eclipse 2019-03
- JRE 1.8.0

### Configurações necessárias para o Windows 10
- Baixar hadoop-3.0.0 do link https://github.com/steveloughran/winutils
- Colocar essa pasta em um local de preferência
    - No meu micro ficou da seguinte forma "C:/hadoop-3.0.0/bin/winutils.exe"
- Agora no Eclipse com o projeto aberto configure acessando > Run / Run Configurations..
- Agora na aba Main clique em Search dentro de Main Class e selecione a class Programa.java

- Na mesma tela acesse a aba Environment > Clique em New... > Em Name adicione: HADOOP_HOME e em Value adicione o caminho do winutil, no meu ficou o seguinte exemplo  C:\winutil

- `Obs.: Não deve colocar a pasta bin do Value.`

### Dependências utilizadas
 - spark-core_2.12, versão 2.4.0 (Necessario para rodar o spark)
 - paranamer, versão 2.8 (necessário por causa da utilização do Java8 e se utilizar o metodo parallelize da class JavaSparkContext)
 


