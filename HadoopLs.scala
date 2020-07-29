package ca.rohith.bigdata.hadoop

import org.apache.hadoop.fs.Path

object HadoopLs extends HdfsClient {
  //hadoop fs -ls /
  fs.listStatus(new Path("/"))
    .map(_.getPath)
    .foreach(println)
}
