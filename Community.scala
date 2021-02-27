import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.SparkContext

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import java.io._

import org.apache.log4j.{Level, Logger}

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
object Community {

  def main(args: Array[String]): Unit = {
    val start_time = System.currentTimeMillis()
    val sparkConf = new SparkConf().setAppName("Communities").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)
    def comapre_function(nodes:(VertexId,VertexId)) : (VertexId, VertexId) = {
      if(nodes._1 < nodes._2) nodes; else nodes.swap
    }
    val input_path = args(0)
   // val input_path="src/main/scala/video_small_num.csv"
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
    var mygraph = Graph(vertices, edges)
    val m = mygraph.numEdges.toDouble
    //////////////////////Traversal/////////////
    val vertices_list = vertices.collect().map(c=>c._1)
    // println(vertices_array)
    val rootnode = vertices_list(0)
    //println(rootnode)
    // var adjacencyMatrixMap= mm.empty[VertexId, Array[VertexId]]
    var adjacencyMatrixMap = mygraph.collectNeighborIds(EdgeDirection.Either).toLocalIterator.toMap

    //adjacencyMatrixMap.foreach(println)
    val den = 2
    def ModularityCalculation(graph: Graph[String, Double], allEdges:RDD[((VertexId,VertexId),Int)]): Double = {
      var multipyM = 2.0 * m
      val connectedGraph = graph.connectedComponents().vertices
        .map(_.swap)
        .groupByKey()
        .map(_._2)
      val degrees: RDD[(VertexId, Int)] = graph.degrees

      val modularity = connectedGraph.map(_.toList.combinations(2).toList)
        .flatMap(r => r)
        .map(r => (r.head, r(1))) join degrees
      val tempvariable1=modularity.map(r => (r._2._1, (r._1, r._2._2))) join degrees
      val calc=tempvariable1.map(r => (comapre_function(r._2._1._1, r._1), (r._2._1._2, r._2._2))) leftOuterJoin allEdges
      val connectedness=calc.map(r => (r._2._1, if(r._2._2.nonEmpty) 1.0 else 0.0))
      val resulting_graph=connectedness.map(r => r._2 - r._1._1*r._1._2/multipyM)
        .collect().sum

      (resulting_graph / multipyM)

    }

    def Betweeness(root: VertexId,vertices_list: Array[VertexId],adjacencyMatrixMap: Map[VertexId, Array[VertexId]]) : mutable.HashMap[(VertexId, VertexId), Double]  = {
      val level = new mutable.HashMap[VertexId, Int]()
      val mylist=new ListBuffer[VertexId]()
      val mysecondlist=new ListBuffer[VertexId]()
      val newEdges = new mutable.HashMap[VertexId, Int]()
      val parents = new mutable.HashMap[VertexId, ListBuffer[VertexId]]()
      val bw = mutable.HashMap[(VertexId, VertexId), Double]()
      val weights = new mutable.HashMap[VertexId, Double]()


      for (i <- vertices_list)
      {

        parents.put(i, ListBuffer())
        newEdges.put(i, 0)
        level.put(i, Int.MaxValue)
        weights.put(i, 0.0)
      }
      mysecondlist+= root
      level(root) = 0
      // BFS
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

      bw
    }
    var increment = 10
    val temporary_betweeness = vertices.map(line => Betweeness(line._1, vertices_list, adjacencyMatrixMap))
      .flatMap(record => record)
      .map(record => if (record._1._1 < record._1._2) (record._1, record._2 / 2); else (record._1.swap, record._2 / 2)) reduceByKey (_ + _) sortByKey()

    var true_betweeness = temporary_betweeness.sortBy(-_._2).collect()

    val graphEdges=temporary_betweeness.map(c=>(c._1,1))

    var sorted_betweeness=true_betweeness

    def count_edges(start_count:Int, increment:Int): Int = {

      var count = start_count
      var updated_graph = mygraph
      var reverse_betweeness = sorted_betweeness

      if (count != 0) {
        val value_max_betweeness = (reverse_betweeness take count map (_._1)).toSet
        reverse_betweeness = reverse_betweeness drop count
        updated_graph = updated_graph.subgraph(r => !value_max_betweeness.contains(comapre_function(r.srcId, r.dstId)))

      }

      var modularity = ModularityCalculation(updated_graph, graphEdges)

      breakable(
        op = while (true) {
          count += increment
          val value_max_betweeness = (reverse_betweeness take increment map (_._1)).toSet
          reverse_betweeness = reverse_betweeness drop increment
          updated_graph = updated_graph.subgraph(r => !value_max_betweeness.contains(comapre_function(r.srcId, r.dstId)))

          //new modularity
          val updatedModularity = ModularityCalculation(updated_graph, graphEdges)
          if (updatedModularity >= modularity) {
            modularity = updatedModularity
          } else break
        }
      )

      count
    }

    def edgeSwap(edge: ListBuffer[(Int, Int)], adjacencyMatrixMap: Set[(VertexId,Array[VertexId])]): ListBuffer[VertexId] = {
      var usr = ListBuffer.empty[Long]

      var node1 = edge.map(c => c._1)
      var node2 = edge.map(c => c._1)

      for (vals <- adjacencyMatrixMap) {
        for (value <- vals._2) {
          usr += value
        }
      }


      usr
    }

    var start_count = 0

    var lastCount = count_edges(start_count, increment)


    val userNeighbourMap=edgeSwap(intersect_users,adjacencyMatrixMap.toSet)
    breakable(
      op = while (true) {

        val temp1 = {
          (sorted_betweeness take lastCount - increment map (_._1)).toSet
        }
        val firstGraph = mygraph.subgraph(line => !temp1.contains(comapre_function(line.srcId, line.dstId)))
        val newCommunities = (firstGraph connectedComponents()).vertices.map(_._2).distinct().count().toInt


        val temp2 = (sorted_betweeness take lastCount map (_._1)).toSet
        val secondGraph = mygraph.subgraph(line => !temp2.contains(comapre_function(line.srcId, line.dstId)))
        val left_communities = (secondGraph connectedComponents()).vertices.map(_._2).distinct().count().toInt

        // Writing file

        if (left_communities - newCommunities <= 1) {
          val finalCommunities = ((firstGraph connectedComponents()).vertices.map(_.swap) groupByKey()).map(_._2.toArray.sorted).sortBy(_.head)
            .map(line => s"""[${line mkString ","}]""")

          val output_file_path = args(1) + "Aayush_Sinha_Community.txt"
          val write_file = new PrintWriter(new File(output_file_path) )
          for(i<-finalCommunities.coalesce(1).collect())
          {
            write_file.write(i+"\n")
          }
          write_file.close()
          break
        }
        else {
          start_count = lastCount - increment
          increment = increment / den
          lastCount = count_edges(start_count, increment)
        }
      }
    )

    val end_time = System.currentTimeMillis()
    println("Time: " + (end_time - start_time) / 1000 + " secs")

  }
}