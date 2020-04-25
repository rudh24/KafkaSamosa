# samosa-taster
Would you like to ~~taste~~ test the samosa?
This is a driver app to test the Kafka wrapper to enable a Pub-Sub system for the collaborative AR uscase.
This implementation strives to be similar to the Pulsar driver [implementation](https://github.com/Manasvini/samosa-tester.git) by [Manasvini](https://github.com/Manasvini)
## Introduction  
The test runner is basically designed to spawn a `p` producer threads, `c` consumer threads and `p+c` location manager threads.  
The location manager takes in a route file as input which simulates movement and triggers topic changes based on the index configuration. A sample config file is included in the `bin/` folder.  
Metrics such as publish rate, end to end latency and subscription changes are written to the output json specified in the config file  
## Building  
```shell  
$ cd path/to/KafkaSamosa
$ mvn install  
```  
 
## Running the code  
```shell  
$ cd bin  
$ ./samosa-taster -c Config.yaml  
```
Note that the config file specifies the paths to the route files which are in the following format:   
```csv  
0.0,0.0  
0.1,0.0  
0.2,0.0  
0.2,0.1  
```
Example files provided are as follows:
1. `Config.yaml` - A basic config file
2. `payload-1Kb.data` - A payload file of size 1Kb
3. `0.0_0.txt` - A sample routes `csv` file.
