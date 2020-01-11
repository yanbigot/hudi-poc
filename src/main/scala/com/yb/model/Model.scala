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
