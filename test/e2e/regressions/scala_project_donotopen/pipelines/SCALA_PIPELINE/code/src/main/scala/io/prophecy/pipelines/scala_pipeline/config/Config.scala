package io.prophecy.pipelines.scala_pipeline.config

import io.prophecy.pipelines.scala_pipeline.config.ConfigStore._
import io.prophecy.pipelines.scala_pipeline.config.Context
import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._
case class Config() extends ConfigBase
