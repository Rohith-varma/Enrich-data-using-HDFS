package ca.rohith.bigdata.hadoop

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait HdfsClient extends App {
    val conf = new Configuration()
    conf.addResource(new Path("/home/rohith/opt/hadoop-2.7.7/etc/cloudera/core-site.xml"))
    conf.addResource(new Path("/home/rohith/opt/hadoop-2.7.7/etc/cloudera/hdfs-site.xml"))

    val fs: FileSystem = FileSystem.get(conf)
}
