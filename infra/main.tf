provider "aws" {
  region = "us-east-2" # Cambia si estás en otra región
}

# 🪣 S3 Bucket (reutilizamos el mismo)
resource "aws_s3_bucket" "datos_bucket" {
  bucket = "proyecnegocios-${random_id.bucket_suffix.hex}"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}
# 

# 🧠 Glue Database 
resource "aws_glue_catalog_database" "base_datos_glue_2" {
  name = "proyecnegocios_db_2"
}


# 📋 Glue Table  
resource "aws_glue_catalog_table" "tabla_ofertas_2" {
  name          = "ofertas_limpias_2"
  database_name = aws_glue_catalog_database.base_datos_glue_2.name
  table_type    = "EXTERNAL_TABLE"

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.datos_bucket.bucket}/ofertas_limpias_2/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      name                  = "SerDeCSV"
      serialization_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde"
    }

    columns {
      name = "id_oferta"
      type = "bigint"
    }
    columns {
      name = "titulo_oferta"
      type = "string"
    }
    columns {
      name = "ciudad"
      type = "string"
    }
    columns {
      name = "region_departamento"
      type = "string"
    }
    columns {
      name = "fecha_publicacion"
      type = "string"
    }
    columns {
      name = "tipo_contrato"
      type = "string"
    }
    columns {
      name = "tipo_jornada"
      type = "string"
    }
    columns {
      name = "modalidad_trabajo"
      type = "string"
    }
    columns {
      name = "salario_monto"
      type = "string"
    }
    columns {
      name = "salario_moneda"
      type = "string"
    }
    columns {
      name = "salario_tipo_pago"
      type = "string"
    }
    columns {
      name = "lenguajes_lista"
      type = "string"
    }
    columns {
      name = "frameworks_lista"
      type = "string"
    }
    columns {
      name = "bases_datos_lista"
      type = "string"
    }
    columns {
      name = "herramientas_lista"
      type = "string"
    }
    columns {
      name = "nivel_ingles"
      type = "string"
    }
    columns {
      name = "nivel_educacion"
      type = "string"
    }
    columns {
      name = "anos_experiencia"
      type = "string"
    }
    columns {
      name = "conocimientos_adicionales_lista"
      type = "string"
    }
    columns {
      name = "edad_minima"
      type = "string"
    }
  }

  parameters = {
    "classification"         = "csv"
    "skip.header.line.count" = "1"
  }
}



# 🔍 Athena Workgroup duplicado (opcional, solo si quieres separarlo)
resource "aws_athena_workgroup" "default_2" {
  name = "primary_2"

  configuration {
    enforce_workgroup_configuration = false

    result_configuration {
      output_location = "s3://${aws_s3_bucket.datos_bucket.bucket}/athena-results-2/"
    }
  }
}
