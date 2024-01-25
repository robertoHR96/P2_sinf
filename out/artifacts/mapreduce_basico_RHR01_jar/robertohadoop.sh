hadoop jar mapreduce-basico-RHR01.jar /input/practica2/ /output_roberto_2023/
hadoop fs -cat /output_roberto_2023/par*
hadoop fs -get /output_roberto_2023/par* ./roberto.txt
