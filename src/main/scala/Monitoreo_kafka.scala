import org.apache.spark.sql.Row
import java.io._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.sql.Timestamp

import java.sql.Timestamp
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.orc._


import org.apache.kudu.spark.kudu._

//import spark.implicits._
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)

object Monitoreo_kafka{
	def main(args: Array[String]) {

		//iniciar variables
		val (log_level, spark_master, path_maestra, path_estructura, path_log_operacional, path_nt, path_batch, kudu_master, ss_batch_secs, spark_log_level, brokers, group_id, security, sasl_mechanism, identificador) = StreamingInit.configInit()
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Variables inicializadas")}
		// if (log_level=="INFO" || log_level=="DEBUG") {Utils.printlog(" INFO StreamingX: Inicio del proceso, topico(s) a consumir: "+ args(0))}
		//iniciar sesion y contexto de spark
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando session spark")}   
		val spark = SparkSession.builder.master(spark_master).getOrCreate()
		spark.sparkContext.setLogLevel(spark_log_level)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Spark iniciado correctamente")} 
		  
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Iniciando contexto de spark")}   
		import spark.implicits._
		val sc = spark.sparkContext
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG: Session y contexto de spark creado")}

		// Inicializando SparkStreaming 
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: Inicializando SparkStreaming")}
		// val streamingContext = new StreamingContext(spark.sparkContext, Milliseconds(ss_batch_secs.toLong))
		if (log_level=="DEBUG") {Utils.printlog(" DEBUG StreamingX: SparkStreaming iniciado correctamente")}

		import spark.implicits._

		val log_operacional_ku = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_log_operacional)).load
		
		val metadata_maestra = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_maestra)).load

		val schema1 = StructType(
			StructField("periodo", IntegerType, true) ::
            StructField("indice", IntegerType, false) ::
			StructField("fecha_hora_kafka", DateType, false) ::
			StructField("fecha_hora_kudu", DateType, true) ::
			StructField("nro_id_sesion", StringType, true) ::
			StructField("fecha_hora_operacion", DateType, true) ::
            StructField("cod_servicio", StringType, true) ::
            StructField("periodo_xml", IntegerType, true) ::
            StructField("fecha_hora_kudu_xml", DateType, true) :: Nil)
        
		// val metadata_estructuras = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_estructura)).load


		val n_df = log_operacional_ku.select("periodo","indice","fecha_hora_kafka","fecha_hora_kudu", "nro_id_sesion","fecha_hora_operacion","cod_servicio")
		
		// val DB_table_1 = "impala::8096_woombat.prueba_monitoreo"
		// val kudu_master_1 = "savcmaster-14f94ce7.ms.davivienda.cloud, savcmaster-1af6073c.ms.davivienda.cloud, savcmaster-2cb71931.ms.davivienda.cloud"
		var df1_temp = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_nt)).load
		df1_temp = df1_temp.where(col("periodo_xml").isNotNull)
		// df1_temp = df1_temp.drop("periodo").withColumn("periodo",col("periodo_xml")).drop("periodo_xml")
		// df1_temp = df1_temp.drop("nro_id_sesion").withColumn("nro_id_sesion",col("nro_id_sesion_xml")).drop("nro_id_sesion_xml")
		df1_temp = df1_temp.select("periodo","indice","fecha_hora_kafka","fecha_hora_kudu","nro_id_sesion","fecha_hora_operacion","cod_servicio")
		val df_compare = n_df.except(df1_temp)
		
		val servicios = metadata_maestra.select("operacion").distinct.collect()
		//Array("SolicitudTarjetaDebito","ListasRestrictivasV1","ConsultaPEP","productosTCXIndicador")
		
		for(val_ser <- servicios){
			// val maestra_temp_c = metadata_maestra.where($"operacion"=== val_ser(0))
			// val c = 1
			// var ind = 0
			// var tabc = "string"
			// while(ind<c){
			var df1 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
			var df_temp = df_compare.where($"cod_servicio"=== val_ser(0))
			val maestra_temp = metadata_maestra.where($"operacion"=== val_ser(0))
			val db = maestra_temp.select("base_datos").collect()(0)(0).toString
			val tb = maestra_temp.select("tabla").collect()(0)(0).toString
			val ss = maestra_temp.select("servicio").collect()(0)(0).toString
			val DB_table = "impala::"+db+"."+tb
			if(tb == ""){
				val c = 1
			}else if(ss == "Membresia Apple"){
				val c = 1
			}else if(tb == "mbaas_areas_geograficas"){
				val c = 1
			}else if(tb == "mbaas_generacion_documentos_desembolso"){
				val c = 1
				// }else if(tb == "mbaas_generacion_paquete_documentos"){
				// 	val c = 1
				// }else if(tb == "mbaas_proceso_biometria"){
				// 	val c = 1
				// }else if(tb == "mbaas_lista_productos_con_membresia"){
				// 	val c = 1
				// }else if(tb == "mbaas_consultar_cliente_pn"){
				// 	val c = 1
				// }else if(tb == "mbaas_autenticacion_ldapidm"){
				// 	val c = 1
			}else{
				val tablas_servicios = spark.read.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> DB_table)).load
				var new_ts = tablas_servicios.select("periodo","nro_id_sesion","fecha_hora_operacion","fecha_hora_kudu")
				new_ts = new_ts.withColumn("nro_id_sesion_xml",col("nro_id_sesion")).drop("nro_id_sesion")
				new_ts = new_ts.withColumn("periodo_xml",col("periodo")).drop("periodo")
				new_ts = new_ts.withColumn("fecha_hora_kudu_xml",col("fecha_hora_kudu")).drop("fecha_hora_kudu")
				new_ts = new_ts.withColumn("fecha_hora_operacion_xml",col("fecha_hora_operacion")).drop("fecha_hora_operacion")
				
				df_temp = df_temp.join(new_ts, df_temp("nro_id_sesion") === new_ts("nro_id_sesion_xml") && df_temp("fecha_hora_operacion") === new_ts("fecha_hora_operacion_xml"),"left")
				df_temp = df_temp.drop("fecha_hora_operacion_xml")
				df_temp = df_temp.drop("nro_id_sesion_xml")

				// df_temp = df_temp.join(new_ts, Seq("nro_id_sesion"), "left")
				df1 = df1.union(df_temp)
					
				// var df_tw = df1.where(col("periodo_xml").isNotNull)
				var df_tw = df1.select("periodo", "fecha_hora_operacion","indice","fecha_hora_kafka","fecha_hora_kudu","nro_id_sesion","cod_servicio","periodo_xml","fecha_hora_kudu_xml")
				df_tw.write.format("org.apache.kudu.spark.kudu").options(Map("kudu.master" -> kudu_master, "kudu.table" -> path_nt)).mode("append").save()
				if (log_level=="INFO" || log_level=="DEBUG" || log_level=="OUTPUT" ){print(" INFO StreamingX: Successfully written: "+df_tw.count()+" records")}
			}
			// }
		}

	}
}