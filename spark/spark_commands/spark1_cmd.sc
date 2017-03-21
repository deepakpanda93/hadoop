//Pre - Partition user data
val userData = sc.sequenceFile[Int, Int]("/tmp/user").persist()
val events = sc.sequenceFile[Int, Int]("/tmp/events")
val joined = userData.join(events)
import org.apache.spark.HashPartitioner
val userData = sc.sequenceFile[Int, Int]("/tmp/users").partitionBy(new HashPartitioner(100)).persist()
//Create a user data RDD partitioned by the hash partition
val joined2 =  userData.join(events)
//Shuffle only event RDD not user data

//== Stored RDD
val a = sc.parallelize(List(1,2,3,4,5,6))
a.saveAstextFile("/tmp/a_text")
// Save 4 , one file per core, split the content in each file and a_text is a directory
// Using paralization of one not the default
val a = sc.parallelize(List(1,2,3,4,5,6), 1)
a.saveAstextFile("/tmp/b_text")
// Save 4 , one file per core and a_text is a directory


val c = sc.parallelize(List(1,2,3,4,5,6),1)
val kvp = c.map((_,"kvp"))
kvp.saveAsSequenceFile("/tmp/kvp_text")
//Sequence file is being created by command above

val d = sc.parallelize(List(1,2,3,4,5,6),1)
d.saveAsObjectFile("/tmp/d_object")
//Sequence file of bytes

