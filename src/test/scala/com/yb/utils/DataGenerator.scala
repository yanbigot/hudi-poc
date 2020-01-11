package com.yb.utils

import java.time.LocalDate

import com.yb.model.Entity

import scala.util.Random

object DataGenerator {

  def generateData(n: Int = 10) ={
    def _validFrom = {
      import java.time.temporal.ChronoUnit.DAYS
      val from = LocalDate.of(1970, 1, 1)
      val to = LocalDate.of(2030, 1, 1)

      val diff = DAYS.between(from, to)
      val random = new Random(System.nanoTime) // You may want a different seed
      from.plusDays(random.nextInt(diff.toInt)) toString
    }
    def _elr(i: Int) = i match {
      case 1 => "FRANCE"
      case 2 => "ESPAGNE"
      case 3 => "ITALIE"
      case 4 => "ALLEMAGNE"
      case 5 => "HOLLANDE"
      case _ => "FRANCE"
    }
    import java.util.UUID.randomUUID
    val uuid = randomUUID().toString
    for(i <- 1 to 10000; j <- 1 to 5) yield {
      Entity(
        ggi = (i * i + j * 10) toString,
        elr = _elr(j),
        hh  = (j % 2 == 0) toString,
        col1 = ("col1_"+i+"_"+j) toString,
        col2 = ("col2_"+i+"_"+j) toString,
        col3 = ("col3_"+i+"_"+j) toString,
        executionId = uuid,
        validFrom = _validFrom
      )
    }
  }
}
