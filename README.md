cp target/bigdata-mr-1.0-SNAPSHOT.jar /home/wucho/projects/softwares/spark-hadoop-docker/share

hadoop fs -rm -r /javastep1
hadoop jar bigdata-mr-1.0-SNAPSHOT.jar info.demo.www.mr.recommend.ItermOccurrence /ratings_teaching /javastep1
hadoop fs -cat /javastep1/part-r-00000


hadoop fs -rm -r /DistributedCacheTest
hadoop jar bigdata-mr-1.0-SNAPSHOT.jar info.demo.www.mr.recommend.DistributedCacheTest /ratings_teaching /DistributedCacheTest
hadoop fs -cat /DistributedCacheTest/part-r-00000
