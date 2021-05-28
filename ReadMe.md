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

启动指令:

```shell script
flume-ng agent -c /usr/local/Cellar/flume/1.9.0_1/conf/ -f /usr/local/Cellar/flume/1.9.0_1/conf/log-kafka.properties -n agent -Dflume.root.logger=INFO,console

```
