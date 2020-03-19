# Spark Streaming

Spark Streaming Scripts

## Commands
- To publish logs to socket `nc -kl 9999 -i 1 < access_log.txt`
- To build Jar `sbt assembly`
- Spark Submit

## Set Up

- Create a checkpoint location `~/code/checkpoint/` 
- Create `twitter.txt` file with Twitter Credentials. For example check `twitter_sample.txt`

# References
- This project contains code snippets from examples from [Taming big data with spark streaming hands on](https://www.udemy.com/course/taming-big-data-with-spark-streaming-hands-on/) Udemy Course