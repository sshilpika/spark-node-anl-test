package gov.anl.alcf

/**
  * Created by Shilpika on 6/14/16.
  */

object `package` {

  case class Time(t: Double) {
    val nanoseconds = t.toLong
    val milliseconds = (t / 1.0e6).toLong

    def +(another: Time): Time = Time(t + another.t)

    override def toString(): String = f"Time(t=$t%.2f, ns=$nanoseconds%d, ms=$milliseconds%d)";
  }

  case class Space(m: Long) {
    val memUsed = m.toDouble
    val memUsedGB = memUsed / math.pow(1024.0, 3)
    val totalMemory = Runtime.getRuntime.totalMemory
    val totalGB = totalMemory / math.pow(1024.0, 3)
    val freeMemory = Runtime.getRuntime.freeMemory
    val freeGB = totalMemory / math.pow(1024.0, 3)

    override def toString(): String = f"Space(memUsedGB=$memUsedGB%.2f, free=$freeGB%.2f, total=$totalGB%.2f)";
  }
  // time a block of Scala code - useful for timing everything!
  // return a Time object so we can obtain the time in desired units

  def performance[R](block: => R): (Time, Space, R) = {
    val t0 = System.nanoTime()
    val m0 = Runtime.getRuntime.freeMemory
    // This executes the block and captures its result
    // call-by-name (reminiscent of Algol 68)
    val result = block
    val t1 = System.nanoTime()
    val m1 = Runtime.getRuntime.freeMemory
    val deltaT = t1 - t0
    val deltaM = m0 - m1
    (Time(deltaT), Space(deltaM), result)
  }

  def quickSort(a:Array[Int]): Array[Int] =  {
    if (a.length < 2) a
    else {
      val pivot = a(a.length / 2)
      quickSort (a filter (pivot>)) ++ (a filter (pivot == )) ++
        quickSort (a filter(pivot <))
    }
  }

  def mergeSort(xs: List[Int]): List[Int] = {
    val n = xs.length / 2
    if (n == 0) xs
    else {
      def merge(xs: List[Int], ys: List[Int]): List[Int] =
        (xs, ys) match {
          case(Nil, ys) => ys
          case(xs, Nil) => xs
          case(x :: xs1, y :: ys1) =>
            if (x < y) x::merge(xs1, ys)
            else y :: merge(xs, ys1)
        }
      val (left, right) = xs splitAt(n)
      merge(mergeSort(left), mergeSort(right))
    }
  }



  /*def quickSort(a: Array[Int],lo: Int, hi: Int):Unit ={
    val pivot = hi
    val wall = lo
    for( curr <- lo until pivot){
      if(a[curr]<a[pivot]){
        //swap wall element and lo element
        swap(a,curr,wall)
        wall++
      }
    }
    swap(a,pivot,wall)
    if((wall-1) > lo)
      quickSort(a,lo,wall-1)
    if(hi > wall+1)
      quickSort(a,wall,hi)

  }

  def swap( x: Array[Int],i:Int, y: Int):Unit = {
    val temp = x[i]
    x[i] = x[y]
    x[y] = temp
  }*/
}

