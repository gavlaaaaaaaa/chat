#!/bin/sh
# $1 - flag for twitter or voce
# $2 - port (and search terms for twitter)
echo "Started ${1} ${2}">> /home/training/sparkjob.txt

# Run twitter
if [ ${1} -eq 1 ]
then
	ssh -n -f training@127.0.0.1 "spark-submit --master local[4] --class scala.streaming.TopWords /home/training/Desktop/spark-project/target/scala-2.10/spark-examples-1.5.1-hadoop2.2.0.jar <consumer key> <consumer secret> <access token> <access token secret> <checkpoint-dir> <tomcat-ip> ${2} > /home/training/out.txt 2>&1 &"
# Run voice
else
	ssh -n -f training@127.0.0.1 "spark-submit --master local[4] --class scala.streaming.StreamingWordConsumer /home/training/Desktop/spark-project/target/scala-2.10/spark-examples-1.5.1-hadoop2.2.0.jar <checkpoint-dir> <voice-ip> <voice-port> <tomcat-ip> ${2} > /home/training/out.txt 2>&1 &"
fi
