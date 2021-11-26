# ML_with_Spark_Streaming
Project carried out as a part of Big Data Course at PES University

## Commands for execution

Command to execute streamer(which streams training dataset):
```console
python3 stream.py -f <dataset> -b <batch_size>
Example: python3 stream.py -f spam -b 80
```

Command to execute testStreamer(which streams testing dataset):
```console
python3 testStream.py -f <dataset> -b <batch_size>
Example: python3 stream.py -f spam -b 80
```

Command to execute client code(i.e., main.py):
```console
/opt/spark/bin/spark-submit main.py -host <hostname> -p <port_no> -w <window_size> -op <operation> -m <model_name> -hash <hashmap_size> 2>log
Example: /opt/spark/bin/spark-submit main.py -host localhost -p 6100 -w 5 -op train -m NB -hash 14 2> log
```
Flags and their meaning:    
-host = Hostname(default=localhost)    
-p = Port number(default=6100)    
-w = Window Interval(in seconds)(default=5s)     
-op = Operation being performed, choose one among test, train or cluster.(default=train)     
-m = Model to be used, choose one among NB, SVM, LR, MLP, PA(i.e., Naive Bayes,Support Vector Machine, Logistic Regression, Multi Layer Perceptron, Passive Aggressive Classifier respectively).(default=NB)   
-hash = Hashmap size(2^(this number)) to be used, default and recommended hash map size on a system of 4GB RAM is 15 and on 2GB RAM it is 10. But please note that decreasing the hash map size may impact the performance of model due to collisions.

Command to plot visualizations(stored in visualizations folder):
```console
python3 ./visualizations/visualizations.py
Example: python3 ./visualizations/visualizations.py
```

Required Modules:
sklearn, numpy