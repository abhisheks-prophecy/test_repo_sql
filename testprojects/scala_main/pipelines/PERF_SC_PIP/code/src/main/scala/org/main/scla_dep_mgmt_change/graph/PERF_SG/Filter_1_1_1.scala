package org.main.scla_dep_mgmt_change.graph.PERF_SG
import io.prophecy.libs._
import org.main.scla_dep_mgmt_change.config.ConfigStore._
import org.main.scla_dep_mgmt_change.config.Context
import org.main.scla_dep_mgmt_change.udfs.UDFs._
import org.main.scla_dep_mgmt_change.udfs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._
object Filter_1_1_1 { def apply(context: Context, in: DataFrame): DataFrame = in.filter(col("cmls_acct_cobrnd_bus_id_drvd") > lit(-10) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister")) and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0))) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister") and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L))) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0))) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister") and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L))) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister"))) and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0)) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister")))) and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0)) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister") and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)))) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister")) and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)))) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister")) and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L)) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0))) and (!col("cmls_acct_ctry_cd_drvd").like("hellosir") or !col("cmls_acct_fundg_srce_cd_enr").like("mister") and (!col("cmls_acct_fundg_srce_cd_drvd").like("transistor") or (col("cmls_acqr_affl_bin_drvd") >= lit(-10) and col("cmls_acqr_affl_bin_drvd") <= lit(100000000000L))) and (col("cmls_acqr_affl_rteg_ntwrk_id") > lit(0) or col("cmls_acqr_aisa_amt_ssbc") < lit(0) or col("cmls_acqr_aisa_amt_usd") > lit(0)))))) }