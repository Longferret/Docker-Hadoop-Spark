import java.io.FileWriter
import java.io.File
import scala.collection.mutable.ListBuffer

val ratings = sc.textFile("hdfs://namenode:9000/data/openbeer/WorkDir/inputs/title.ratings.tsv")
val principals = sc.textFile("hdfs://namenode:9000/data/openbeer/WorkDir/inputs/title.principals.tsv")

// --- STEP 1: format data to have TITLE ACTOR & TITLE RATING
val title_actor = principals.map( 
    // map to split file with tab
    line => line.split("\t")).filter(
        // filter to have only actor and actress
        arr => arr.length > 3 && (arr(3)=="actor" || arr(3)=="actress")).map(
            // map to format to only have (title,actor)
            arr => (arr(0),arr(2)))

val title_rating = ratings.map(
    // map to split file with tab
    line => line.split("\t")).filter(
        // filter to delete first column (ineficient ?)
        !_.sameElements(Array("tconst", "averageRating","numVotes"))).map(
            // map to format to only have (title,rating)
            arr => (arr(0),arr(1)))



// --- STEP 2: merge the 2 RDD to have  TITLE RATING ACTOR1 ACTOR2 ..
val title_rating_actor = (title_actor++title_rating).reduceByKey(
    // Reduce, Always append actor at the end (rating is first)
    (actor1,actor2) => {
        if(actor1.startsWith("nm")){
            actor2 +"\t"+ actor1
        }
        else{
            actor1 +"\t"+ actor2
        }
        })


// --- STEP 3: map the above value to get  ACTOR RATING 1 (actor can appear several times)
val tmpList = ListBuffer[(String, String)]()
val actor_rating = title_rating_actor.flatMap{
    case (title,data) => {
        // key data in token form
        var tokens = data.split("\t")
        // if 1 token, it means there is a rating but no actor, no return value
        if(tokens.length != 1){
            // create an empty list to map 1 (TITLE RATING ACTOR1 ACTOR2 ..) to multiple (ACTOR1 RATING) (ACTOR2 RATING) ..
            tmpList.clear()
            val rating = tokens(0)
            // Ensure that it is the rating first (could be an other actor if there were no rating for that film)
            if(!rating.startsWith("nm")){
                val actors = tokens.drop(1)
                // loop over actors to create (ACTOR1 RATING 1) (ACTOR2 RATING 1) ..
                for (n <- actors){
                    tmpList += ((n,rating+"\t1"))
                }
            }
            tmpList
        }
        else{
            Nil
        }
}}

// --- STEP 4: Add all ratings together and the number of ratings
val actor_rating_sum = actor_rating.reduceByKey( (rating1,rating2) => {
    val tokensr1 = rating1.split("\t")
    val tokensr2 = rating2.split("\t")
    String.valueOf(tokensr2(0).toDouble + tokensr1(0).toDouble) + "\t" + String.valueOf(tokensr2(1).toInt + tokensr1(1).toInt)
})

// --- STEP 5: Divide the total rating per the number of films
val actor_avg_rating = actor_rating_sum.map{ case (actor,data) => {
    val tok = data.split("\t")
    (actor,String.valueOf(tok(0).toDouble /tok(1).toDouble))
}}


// --- STEP 6: Write file 
val fileWriter = new FileWriter(new File("outputs/SCALA_AvgRating.txt"))
actor_avg_rating.collect().foreach{case(a,b) => fileWriter.write(s"${a}\t${b}\n")}
fileWriter.close()


