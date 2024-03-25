val NUM_SAMPLES = 10000
val count = sc.parallelize(1 to NUM_SAMPLES).filter { _ =>
  val x = math.random
  val y = math.random
  val z = x * x + y * y 
  println(s"$z < 1 = ${x < 1}")
  z < 1
}.count()
println(s"Pi is roughly ${4.0 * count / NUM_SAMPLES}")