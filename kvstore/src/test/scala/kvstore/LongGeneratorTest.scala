package kvstore

class LongGeneratorTest extends munit.FunSuite {

  test("Generator starts at zero by default"){
    assertEquals(new LongGenerator().next(), 0L)
  }

  test("Generator can start negative") {
    assertEquals(new LongGenerator(-1L).next(), -1L)
  }

  test("Generator increments by 1") {
    val lg = new LongGenerator(-1L)
    lg.next()
    assertEquals(lg.next(), 0L)
  }

  test("LongGenerator.from changes the next result") {
    val lg = new LongGenerator(-1L)
    assertEquals(lg.next(), -1L)
    assertEquals(lg.next(), 0L)
    assertEquals(lg.from(11L).next(), 11L)
    assertEquals(lg.from(-19L).next(), -19L)
    assertEquals(lg.next(), -18L)
  }

}
