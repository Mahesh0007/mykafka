 if [ -z "$1"]
  then
   echo "Usage: Zookeeper: $1 GroupId: $2 Topic: $3 WindowSize: $4 Wait-Time:$5"
fi
echo "Zookeeper: $1 GroupId: $2 Topic: $3 WindowSize: $ Wait-Time:$5"
java -cp target/uber-SimpleMvgAvg-1.0-SNAPSHOT.jar com.nVidia.examples.zkconsumer.simplemovingavg.SimpleMovingAvgZkConsumer $1 $2 $3 $4 $5
