#All helper functions are defined here


def addArguments(parser):
    """Adds command line arguments to parser object.
    Input; parser(expects a parser argument)
    Output: Adds various necessary command line arguments
    """
    parser.add_argument('--host-name', '-host', help='Hostname', required=False,
                    type=str, default="localhost") 
    parser.add_argument('--port-number', '-p', help='Port Number', required=False,
                        type=int, default=6100) 
    parser.add_argument('--window_interval', '-w', help='Window Interval', required=False,
                        type=int, default=5) 
    parser.add_argument('--op', '-op', help='Operation being performed', required=False,
                        type=str, default="train") # op can be 1 among 'train','test'
    parser.add_argument('--proc', '-proc', help='Type of Preprocessing Performed', required=False,
                        type=str, default="tf")# choose 1 among tf,word2vec,glove,use,elmo or bert
    parser.add_argument('--sampleFraction','-sf',help='Sampling fraction for every batch',required=False,
                        type=float,default=1.0) # Use this when each batch size is large
    parser.add_argument('--model', '-m', help='Choose Model', required=False,
                        type=str, default="NB")#model can be 1 among 'NB','SVM','LR','MLP' or 'PA'
    parser.add_argument('--cluster', '-c', help='Enable clustering',
                        required=False, type=bool, default=False)
    parser.add_argument('--explore', '-e', help='Enable data exploration',
                        required=False, type=bool, default=False)
    parser.add_argument('--hashmap_size', '-hash', help='Hash map size to be used', required=False,
                        type=int, default=14)#hashmap_size=2^(this number)
                        # default and recommended hash map size on a system of 4GB RAM is 14 and on 2GB RAM 
                        # it is 10. But please note that decreasing the hash map size may impact the performance 
                        # of model due to collisions.


def printMetrics(testingParams,modelChosen):
      total_samples=testingParams['tp']+testingParams['tn']+testingParams['fp']+testingParams['fn']
      accuracy=(testingParams['tp']+testingParams['tn'])/total_samples
      precision=(testingParams['tp'])/(testingParams['tp']+testingParams['fp'])
      recall=(testingParams['tp'])/(testingParams['tp']+testingParams['fn'])
      f1=(2*precision*recall)/(precision+recall)

      print(f"Model Name: {modelChosen}")
      print("----------------------------------")
      print("")
      print("Confusion Matrix:")
      print("---------------")
      print(f"{testingParams['tp']} | {testingParams['fn']}")
      print("---------------")
      print(f"{testingParams['fp']} | {testingParams['tn']}")
      print("---------------")
      print("")
      print("Accuracy: {:.4f}".format(accuracy))
      print("F1 Score: {:.4f}".format(f1))