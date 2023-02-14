package io.prophecy.pipelines.scala_livy.config

import io.prophecy.pipelines.scala_livy.config.ConfigStore._
import io.prophecy.pipelines.scala_livy.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config() extends ConfigBase
