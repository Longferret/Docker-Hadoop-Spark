
import java.io.FileWriter
import java.io.File
import scala.collection.mutable.ListBuffer

val ratings = sc.textFile("hdfs://namenode:9000/data/openbeer/WorkDir/inputs/title.ratings.tsv")
val principals = sc.textFile("hdfs://namenode:9000/data/openbeer/WorkDir/title.principals2.tsv")

val sep_actor = "nm0000658"

// --- STEP 1: format data to have TITLE ACTOR DISTANCE
val title_actors_dist = principals.map( 
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


// --- STEP 2: spread value among films
// 1. Get all actors for 1 film + the minimum dist
val reduced = title_actors_dist.reduceByKey(
    (data1,data2) => { 
        val tokens1 = data1.split("\t")
        var dist1 = tokens1(1)
        var actor1 =  tokens1(0)
        val tokens2 = data2.split("\t")
        var dist2 = tokens2(1)
        var actor2 =  tokens2(0)
        if(dist2.toInt > dist1.toInt ){
            actor1 + "," + actor2 +"\t" + String.valueOf(dist1.toInt+1)
        }
        else{
            actor1 + "," + actor2 +"\t"+ String.valueOf(dist2.toInt+1)
        }
    })
// 2. Replace dist by min_dist
val map_reduced = reduced.map{
    case(title,data) => {
        
    }
}


/*
// --- STEP3: spred value among actors
// 1. Get all films for 1 actor + the minimum dist
// 2. Replace dist by min_dist



// --- STEP 2: Format data and init distance to get TITLE DISTANCE ACTOR1 ACTOR2 ACTOR3 ...
// where distance is 2147483647 for infinity or 0 if the actor is in the one of the separation degree
var dist = 2147483647
val title_actors_dist = title_actor_dist.reduceByKey(
    (actor1,actor2) => {
        actor1 + "\t" + actor2
    }).map{
        // map to add a distance
        case (title,data) => {
            dist = 2147483647
            for(actor <- data.split("\t")){
                if(actor == sep_actor){
                    dist = 0
                }
            }
            (title,dist+"\t"+data)
        }
    }


// --- STEP 3: Map to get key(title) data(actor \t distance)
val tmpList = ListBuffer[(String, String)]()
val actor_titles_dist = title_actors_dist.flatMap{
    case (title,data) => {
        var tokens = data.split("\t")
        var dist = tokens(0)
        val actors = tokens.drop(1)
        tmpList.clear()
        for(actor <- actors){
            tmpList += ((title,actor+"\t"+dist))
        }
        tmpList
    }
}*/


/*var i = 0
while(i<10){
    println(s"${i}")
    i += 1
}
*/
// --- STEP 6: Write file 
val fileWriter = new FileWriter(new File("test.txt"))
reduced.collect().foreach{case(a,b) => fileWriter.write(s"${a}\t${b}\n")}
fileWriter.close()


