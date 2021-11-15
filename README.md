# ML_with_Spark_Streaming
Project carried out as a part of Big Data Course at PES University

## Commands to execute

Command to execute streamer:
```console
python3 stream.py -f <dataset> -b <batch_size>
Example: python3 stream.py -f spam -b 3
```

Command to execute client code:
```console
/opt/spark/bin/spark-submit main.py <hostname> <portNumber> <batch_size> 2> log
Example: /opt/spark/bin/spark-submit main.py localhost 6100 3 2> log
```