package kvstore

class LongGenerator(val startWith: Long = 0L) {

  private val counter: Array[Long] = Array.from(List(startWith))  // I don't use vars

  def from(startWith: Long): this.type = { counter(0) = startWith; this }

  def next(): Long = {
    val ret = counter.head
    counter(0) += 1L
    ret
  }

}
