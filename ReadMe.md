# HaloRecSys 

HaloRecSys is a simple movie recommendation web project.



## Dependency

- Mongodb

  启动指令：

  ```shell
  sudo mongod --dbpath /usr/local/mongodb/data/ --logpath /usr/local/mongodb/data/logs/mongodb.log --config /usr/local/mongodb/data/mongodb.conf
  sudo mongod --config /usr/local/mongodb/data/mongodb.conf
  ```

  

- Redis

- Spark 2.4.3

- Scala 2.11.8

- zookeeper
    
    启动方式:
```shell script
 zkServer start
```
   
- kafka

    启动指令：
 ``` shell script 
查看topic 
kafka-topics --list --zookeeper localhost:2181/kafka

创建topic
kafka-topics --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic recommender
kafka-topics --create --zookeeper localhost:2181/kafka --replication-factor 1 --partitions 1 --topic log

启动kafka (fuck，必须以管理员身份启动，一堆坑)
sudo kafka-server-start -daemon /usr/local/etc/kafka/server.properties

创建生产者topic
kafka-console-producer --broker-list localhost:9092 --topic recommender

消费者topic
kafka-console-consumer --bootstrap-server localhost:9092 --topic log
  ```

- flume

配置文件(不要把注释写在一行最后，调了我一天的时间)
```shell script
agent.sources = exectail
agent.channels = memoryChannel
agent.sinks = kafkasink

# For each one of the sources, the type is defined
agent.sources.exectail.type = exec
agent.sources.exectail.command = tail -f /Volumes/Study/RS/Projects/HaloRecSys/BusinessServer/src/main/log/agent.log
agent.sources.exectail.interceptors = i1
agent.sources.exectail.interceptors.i1.type = regex_filter
agent.sources.exectail.interceptors.i1.regex = .+MOVIE_RATING_PREFIX.+

# The channel can be defined as follows.
agent.sources.exectail.channels = memoryChannel

# Each sink's type must be defined
agent.sinks.kafkasink.type = org.apache.flume.sink.kafka.KafkaSink
#kafka topic name
agent.sinks.kafkasink.kafka.topic = log
agent.sinks.kafkasink.kafka.bootstrap.servers = localhost:9092
agent.sinks.kafkasink.kafka.producer.acks = 1
agent.sinks.kafkasink.kafka.flumeBatchSize = 10

#Specify the channel the sink should use
agent.sinks.kafkasink.channel = memoryChannel

# Each channel's type is defined.
agent.channels.memoryChannel.type = memory

# Other config values specific to each type of channel(sink or source)
# can be defined as well
# In this case, it specifies the capacity of the memory channel
agent.channels.memoryChannel.capacity = 1000
```

启动指令:
```shell script
sudo flume-ng agent -c /usr/local/Cellar/flume/1.9.0_1/conf/ -f /usr/local/Cellar/flume/1.9.0_1/conf/log-kafka.properties -n agent -Dflume.root.logger=INFO,console
```


# Torch server
## 启动指令
torchserve --stop
torch-model-archiver --model-name ncf --version 1.0 --model-file NeuralCF.py --serialized-file TrainedModels/NCF.pth --handler handler.py
mv ncf.mar model_store/
torchserve --start --model-store model_store --models ncf=ncf.mar