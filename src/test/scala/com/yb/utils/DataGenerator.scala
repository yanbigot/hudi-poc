package com.yb.utils

import java.time.LocalDate
import java.util.UUID

import com.yb.model.{Entity, WorkforceAdministationBvEntity}

import scala.util.Random

object DataGenerator {

  def generateRandomData(n: Int = 10) ={
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
    for(i <- 1 to n; j <- 1 to 5) yield {
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

  def generateBusinessViewData(n: Int = 10, m: Int = 5) = {
    def _validDate(from: LocalDate = LocalDate.of(1970, 1, 1), to: LocalDate = LocalDate.of(2030, 1, 1)) = {
      import java.time.temporal.ChronoUnit.DAYS
      val diff = DAYS.between(from, to)
      val random = new Random(System.nanoTime)
      from.plusDays(random.nextInt(diff.toInt))
    }
    def _elr(i: Int) = i match {
      case 1 => "FRANCE"
      case 2 => "ESPAGNE"
      case 3 => "ITALIE"
      case 4 => "ALLEMAGNE"
      case 5 => "HOLLANDE"
      case _ => "FRANCE"
    }
    def _entityName(i: Int) = i match {
      case 1 => "EMPLOYEE"
      case 2 => "CONTRACT"
      case 3 => "EDUCATION"
      case 4 => "JOB"
      case 5 => "ABSENCE"
      case _ => "EMPLOYEE"
    }

    val executionId = _validDate() toString

    for(i <- 1 to n; j <- 1 to m) yield {
      WorkforceAdministationBvEntity(
        rowId = UUID.randomUUID().toString,
        mergeKey= s"ggi:${i toString},elr:${_elr(i)},hh:${(j % 2 == 0) toString}",
        entityKey= "",
        domainName="WORKFORCE_ADMINISTRATION",
        entityName= _entityName(j),
        integrationDate= System.currentTimeMillis() toString,
        sourceAppCreationDate= _validDate() toString,
        validFromDate= _validDate() toString,
        executionId= executionId,
        version= "",
        ggi= i toString,
        elr= _elr(i),
        hh= (j % 2 == 0) toString,
        functionalData= Random.alphanumeric.take(50).mkString
      )
    }
  }
}
