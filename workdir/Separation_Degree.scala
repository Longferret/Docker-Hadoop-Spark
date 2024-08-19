import java.io.FileWriter
import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD

def step(inf_count:Int, current_dist:Int, stop:Boolean, start_rdd:RDD[(String,String)]): RDD[(String,String)] = {
    val inf = 2147483647
    // Base case
    if(stop){
        // Return
        start_rdd
    }

    // Recursive case
    else{
        /* 2 : Reduce by key to obtain 
            key : title
            value : actor1,..., actorN,dist
        */
        val title_actors_dist_reduced_by_key = start_rdd.reduceByKey(
            (line1, line2) =>{
                val array1 = line1.split("\t")
                var dist1 = array1(1)
                var act1 = array1(0)

                val array2 = line2.split("\t")
                var dist2 = array2(1)
                var act2 = array2(0)

                // both are in the same movie, but there is no kevin bacon
                if(dist1.toInt >= inf && dist2.toInt >= inf){
                    act1 + "," + act2  + "\t" + String.valueOf(inf)
                }   

                // at least one of the actors is linked to kevin bacon
                else{
                    if(dist2.toInt > dist1.toInt){
                        act1 + "," + act2  + "\t" + String.valueOf(dist1.toInt)
                    }
                    else{
                        act2 + "," + act1  + "\t" + String.valueOf(dist2.toInt)
                    }
                }
            }
        )
        /* 3 : Flat map to obtain
            key :  actor
            value : title, dist
        */
        
        val actor_titles_dist = title_actors_dist_reduced_by_key.flatMap{ 
            case(title, data) => {
                val array = data.split("\t")
                val actors = array(0).split(",")
                val closest_actor = actors(0)
                val min_dist = array(1)
                actors.map(
                    actor => {
                        if(min_dist.toInt >= inf){
                            (actor, title + "\t" + String.valueOf(inf))
                        }
                        else{
                            if(actor == closest_actor){
                                (actor, title + "\t" + min_dist)
                            }
                            else(
                                (actor, title + "\t" + String.valueOf(min_dist.toInt + 1))
                            )
                        } 
                    }
                )
            }
        } 

        /* 4 : Reduce by key to obtain
            key : actor :
            value : title1, ..., titleN, dist
        */
        
        val actor_titles_dist_reduced_by_key = actor_titles_dist.reduceByKey(
            (line1, line2) =>{
                val array1 = line1.split("\t")
                var dist1 = array1(1)
                var title1 = array1(0)

                val array2 = line2.split("\t")
                var dist2 = array2(1)
                var title2 = array2(0)
                if(dist1.toInt >= inf && dist2.toInt >= inf){
                    title1 + "," + title2  + "\t" + String.valueOf("2147483647")
                }
                else{
                    if(dist2.toInt > dist1.toInt){
                        title1 + "," + title2  + "\t" + String.valueOf(dist1.toInt)
                    }
                    else{
                        title1 + "," + title2  + "\t" + String.valueOf(dist2.toInt)
                    }
                }
            }
        )

        /* 5 : Flat map to obtain
            key : title
            data : actor, dist
        */
        val title_actors_dist = actor_titles_dist_reduced_by_key.flatMap{
            case(actor, data) => {
                var array = data.split("\t")
                var titles = array(0).split(",") // array(1) is dist
                titles.map(title => (title,  actor+ "\t" + array(1)))
            }
        }

        // Return
        /*
        Should we stop ?
        At some point the only actors left who still have an infinite distance
        are those not linked to KB whatsoever.
        Therefore we simply count the amount of actors who still have an infinite
        distance, which should decrease after each step.
        If at some step it remains equal, that means we did the whole 
        graph.
        */
        //title_actors_dist_reduced_by_key
        
        val new_inf_count = title_actors_dist.filter{case(title,data) =>{
            data.split("\t")(1).toInt >= inf
        }}.count()

        if(new_inf_count.toInt >= inf_count){//stop
            step(new_inf_count.toInt, current_dist+1, true, title_actors_dist)
        }
        else{// keep going
            step(new_inf_count.toInt, current_dist+1, false, title_actors_dist)
        }
        
    }
}


val principals = sc.textFile("hdfs://namenode:9000/data/openbeer/WorkDir/inputs/title.principals.tsv")

// nm0791570
val sep_actor = "nm0000658"
//val max_depth = 3
val inf = 2147483647


// Initial rdd for formating

val init_rdd = principals.map( 
    // map to split file with tab
    line => line.split("\t")).filter(
        // filter to have only actor and actress
        array => array(3)=="actor" || array(3)=="actress").map(
            // map to format to only have (title,actor) and add distance at the end (0 for sep actor and 2147483647 for others)
            array => {
                if(array(2) == sep_actor){
                    (array(0),array(2)+"\t0")
                }
                else{
                    (array(0),array(2)+"\t2147483647")
                }
            })

// Compute all the KB degrees
val result = step(inf, 1, false, init_rdd)


/*
Let's do one more round to only have (actor, dist)
Remove titles, we no longer need them at this point
Now have (actor, dist)
*/

val rdd1 = result.map{
    case(title, data) => {
        val actor = data.split("\t")(0)
        val dist = data.split("\t")(1)
        (actor,dist)
    }
}


/*
Reduce by key to only have one instance of each actor
keeping only the minimum distance
*/

val rdd2 = rdd1.reduceByKey(
    (line1,line2) =>{
        if(line2.toInt > line1.toInt){
            line1//String.valueOf(line1.toInt)
        }
        else{
            line2//String.valueOf(line2.toInt)
        }
    }
) 

// Print to file
val fileWriter = new FileWriter(new File("SCALA_SEP_DEG.txt"))
rdd2.collect().foreach{case(a) => fileWriter.write(s"${a}\n")}
fileWriter.close()




