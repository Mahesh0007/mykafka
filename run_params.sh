echo "Zookeeper: $1 Topic: $2 Sync: $3 Delay: $4 Count: $5"
java -cp target/uber-mykafkapractice-1.0-SNAPSHOT.jar com.nVidia.examples.oldproducer.simplecounter.SimpleCounterOldProducer $1 $2 $3 $4 $5
