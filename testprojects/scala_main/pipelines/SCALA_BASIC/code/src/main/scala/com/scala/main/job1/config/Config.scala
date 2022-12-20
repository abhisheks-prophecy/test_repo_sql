package com.scala.main.job1.config

import com.scala.main.job1.config.ConfigStore._
import com.scala.main.job1.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config(c_test: String = "random") extends ConfigBase
