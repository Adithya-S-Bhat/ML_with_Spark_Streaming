# ML_with_Spark_Streaming
Project carried out as a part of Big Data Course at PES University

## Commands for execution

Command to execute streamer:
```console
python3 stream.py -f <dataset> -b <batch_size>
Example: python3 stream.py -f spam -b 5
```

Command to execute client code(i.e., main.py):
```console
/opt/spark/bin/spark-submit main.py -host <hostname> -p <port_no> -b <batch_size> -t <isTest> -m <model_name> 2>log
Example: /opt/spark/bin/spark-submit main.py -host localhost -p 6100 -b 5 -t False -m 'NB' 2> log
```
Flags and their meaning:    
-host = Hostname    
-p = Port number    
-b = Batch Size     
-t = True when Testing dataset is streamed and false when training dataset is streamed.     
-m = Model to be used, choose one among NB, SVM, LR, MLP, PA.
