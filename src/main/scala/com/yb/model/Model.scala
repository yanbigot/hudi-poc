package com.yb.model

case class Entity(ggi: String,
                  elr:String,
                  hh: String,
                  col1: String,
                  col2: String,
                  col3: String,
                  sysDate: String = System.nanoTime() toString,
                  executionId: String,
                  validFrom: String)


case class WorkforceAdministationBvEntity(
                   rowId: String,
                   mergeKey: String,
                   entityKey: String,
                   domainName: String,
                   entityName: String,
                   integrationDate: String,
                   sourceAppCreationDate: String,
                   validFromDate: String,
                   executionId: String,
                   version: String,
                   ggi: String,
                   elr: String,
                   hh: String,
                   functionalData: String
                   )
