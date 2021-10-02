# Databricks notebook source


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, IntegerType, FloatType, TimestampType,ByteType, ShortType, LongType, BooleanType,DateType,DecimalType,DoubleType
def mapToSparkDataType(dtype) :
  schema_Mapping= { "int":IntegerType(),
                   "string":StringType(),
                   "varchar":StringType(),
                   "nvarchar":StringType(),
                   "bigint":LongType(),
                   "long":LongType(), 
                   "bool":BooleanType(),
                   "boolean":BooleanType(),
                   "date":DateType(),
                   "decimal":DecimalType(10,0),
                   "datetime":TimestampType(),
                   "timestamp":TimestampType(),
                   "smallint":ShortType(),
                   "tinyint":ShortType(),
                   "bit":ShortType(),
                   "double":DoubleType(),
                   "char(2)":StringType(),
                   "char(4)":StringType(),
                   "float":FloatType()
                   }
  
  if('(' in dtype and 'decimal' in dtype):
    precesion = int(dtype[dtype.index('(')+1:dtype.index(',')])
    scale = int(dtype[dtype.index(',')+1:dtype.index(')')])
    rs= DecimalType(precesion,scale)
  else:
    rs = schema_Mapping[dtype]
  return rs

# COMMAND ----------

def convertToSparkSchema(schema_df):
  field = {}
  struct_field = []
  schema_type = spark.createDataFrame(schema_df, ['column_name','type_name','nullable'])
  for row in schema_type.rdd.collect():
    field[row.column_name]=mapToSparkDataType(row.type_name)
  for x in field:
    struct_field.append(StructField(x,field[x],False))
  return StructType(fields = struct_field)