### ОС: CentOS7
### Hadoop 2.10.1
### Краткая инструкция по установке и запуску:
Установить и настроить `hadoop/yarn`

Установить python3: `yum install python3`

Запустить hadoop командой: `/opt/hadoop-2.10.1/sbin/start-dfs.sh`

Запустить yarn командой:   `/opt/hadoop-2.10.1/sbin/start-yarn.sh`

Скомпилировать исходный проект с помощью сборщика **maven**

Запустить файл генерации командой: `python3 generate.py`

Скопировать полученный в файл в hdfs командой: `hdfs dfs -put input input`

Запустить hadoop в директории проекта в псевдораспределенном режиме командой:
`hadoop jar target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar input output metricNames 10 s`
