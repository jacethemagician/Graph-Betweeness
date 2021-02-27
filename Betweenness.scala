  
import java.io.{File, PrintWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import java.io._

import scala.collection.mutable.{Map => mm}
import scala.collection.mutable

object Betweenness{
  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val sparkcont = new SparkConf().setAppName("Girvin Neumann Algorithm").setMaster("local[4]")
    var sc = new SparkContext(sparkcont)
    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val input_path = args(0)
    var data = sc.textFile(input_path)
    var header = data.first()
    data = data.filter(x => x != header)
    var users = data.map(_.split(",")).map(c => (c(0).toInt, c(1).toInt)).groupByKey().sortByKey()
      .map { case (k, v) => (k, v.toSet) }.collect()
    //users.foreach(println)
    val filtered_users = users.filter(_._2.size >= 7)
    //filtered_users.foreach(println)

    var intersect_users = ListBuffer.empty[(Int, Int)]

    for (i <- filtered_users) {
      for (j <- filtered_users) {
        if (i != j && i._1 < j._1) {
          var a = i._2.intersect(j._2)
          if (a.size >= 7) {
            intersect_users += ((i._1, j._1))
          }
        }
      }
    }
    //intersect_users.foreach(println)
    //println("size", intersect_users.size)

    val distinct_users = intersect_users.flatMap { case (a, b) => List(a, b) }.distinct
    //distinct_users.foreach(println)


    //                  MAKING GRAPH                    //

    val intersect_users_rdd = sc.parallelize(intersect_users)
    val distinct_user_rdd = sc.parallelize(distinct_users)

    val vertices: RDD[(VertexId, String)] = distinct_user_rdd.map(line => (line, line.toString))

    val edges = intersect_users_rdd.map({ case (id1, id2) => Edge(id1, id2, 0.0) })
    var graph = Graph(vertices, edges)
    //println("num edges = " + graph.numEdges)
    //println("num vertices = " + graph.numVertices)
    //Traversal/////////////
    val vertices_list = vertices.collect().map(c=>c._1)
    // println(vertices_array)
    val rootnode = vertices_list(0)
    //println(rootnode)
    // var adjacencyMatrixMap= mm.empty[VertexId, Array[VertexId]]
    var adjacencyMatrixMap = graph.collectNeighborIds(EdgeDirection.Either).toLocalIterator.toMap

    //adjacencyMatrixMap.foreach(println)

    var neighbours = Map.empty[Long, List[Long]]
    for (i <- adjacencyMatrixMap) {
      neighbours += i._1 -> i._2.toList
    }
    //println(neighbours)
//    var treemap = Map.empty[Long, Long]
    var temp = neighbours

    val final_betweeness = vertices.map(line => Betweeness(line._1,vertices_list,adjacencyMatrixMap))
      .flatMap(record => record)
      .map(record =>  if (record._1._1 < record._1._2)( record._1, record._2 / 2); else (record._1.swap,(record._2/2)))
      .reduceByKey(_+_).sortByKey()
    //println(sortedBtweeness.count())
    //   final_betweeness.foreach(println)

    var result =final_betweeness.collect().sorted
    var resultRDD = sc.parallelize(result)
    var finalresult = resultRDD.map(x =>("("+x._1._1+"," + x._1._2 +","+x._2+")"))
    //result.foreach(println)
    val output_file_path = args(1) + "Aayush_Sinha_Betweenness.txt"
    val write_file = new PrintWriter(new File(output_file_path) )
    for(i<-finalresult.collect())
      {
        write_file.write(i+"\n")
      }
    write_file.close()

    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")
  }


  def Betweeness(root: VertexId,vertices_list: Array[VertexId],adjacencyMatrixMap: Map[VertexId, Array[VertexId]]) : mutable.HashMap[(VertexId, VertexId), Double]  = {
    val level = new mutable.HashMap[VertexId, Int]()
    val newEdges = new mutable.HashMap[VertexId, Int]()
    val parents = new mutable.HashMap[VertexId, ListBuffer[VertexId]]()
    val weights = new mutable.HashMap[VertexId, Double]()
    for (i <- vertices_list)
    {

      parents.put(i, ListBuffer())
      newEdges.put(i, 0)
      level.put(i, Int.MaxValue)
      weights.put(i, 0.0)
    }
    val mylist=new ListBuffer[VertexId]()
    val mysecondlist=new ListBuffer[VertexId]()
    mysecondlist+= root
    level(root) = 0
 //                BFS                  //
    while (mysecondlist.nonEmpty)
    {
      val currentNode = mysecondlist.remove(0)
      mylist+=currentNode
      val adj = adjacencyMatrixMap(currentNode)
      for (child <- adj)
      {
        if (level(child) == Int.MaxValue)
        {
          level(child) = level(currentNode) + 1
          mysecondlist+= child
        }
        if (level(child) == level(currentNode) + 1)
        {
          newEdges(child) = newEdges(child) + 1
          parents(child).+=(currentNode)
        }
      }
    }
    val bw = mutable.HashMap[(VertexId, VertexId), Double]()
    while (mylist.nonEmpty)
    {

      val removed_element = mylist.remove(mylist.length-1)
      weights(removed_element) = weights(removed_element) + 1
      for (child <- parents(removed_element))
      {
        if (level(child) == level(removed_element) - 1)
        {
          bw.put((child, removed_element), weights(removed_element) / newEdges(removed_element))
          weights(child) = weights(child) + weights(removed_element) / newEdges(removed_element)
        }
      }
    }
    //bw.foreach(println)
    //println("jjdnfkdj",bw.size)

    bw
  }

}