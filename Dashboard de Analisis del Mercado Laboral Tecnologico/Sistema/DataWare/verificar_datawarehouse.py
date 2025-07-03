import boto3
import pandas as pd
from datetime import datetime

def verificar_estado_datawarehouse():
    """
    Verifica el estado actual de tu Data Warehouse
    """
    print("🏗️ VERIFICANDO ESTADO DEL DATA WAREHOUSE")
    print("=" * 50)
    
    status = {"kinesis": False, "dynamodb": False, "lambda": False}
    
    # 1. Verificar Kinesis Stream
    kinesis = boto3.client('kinesis', region_name='us-east-2')
    try:
        response = kinesis.describe_stream(StreamName='streamOfertas')
        print(f"✅ Kinesis Stream: {response['StreamDescription']['StreamStatus']}")
        print(f"   📊 Shards: {len(response['StreamDescription']['Shards'])}")
        status["kinesis"] = True
    except Exception as e:
        print(f"❌ Kinesis Stream: {e}")
    
    # 2. Verificar DynamoDB (con nombre correcto)
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-2')
    dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-2')
    
    # Intentar ambos nombres de tabla
    table_names = ['ofertas_trabajo', 'OfertasTrabajo']
    
    for table_name in table_names:
        try:
            response = dynamodb_client.describe_table(TableName=table_name)
            print(f"✅ DynamoDB Table '{table_name}': {response['Table']['TableStatus']}")
            
            # Contar registros
            table = dynamodb_resource.Table(table_name)
            scan_response = table.scan(Select='COUNT')
            print(f"   📊 Total registros: {scan_response['Count']}")
            status["dynamodb"] = True
            break
            
        except Exception as e:
            print(f"❌ DynamoDB Table '{table_name}': {e}")
    
    # 3. Verificar Lambda
    lambda_client = boto3.client('lambda', region_name='us-east-2')
    try:
        response = lambda_client.get_function(FunctionName='ofertas-processor')
        print(f"✅ Lambda Function: {response['Configuration']['State']}")
        print(f"   ⚡ Runtime: {response['Configuration']['Runtime']}")
        status["lambda"] = True
    except Exception as e:
        print(f"❌ Lambda Function: {e}")
    
    # 4. Resumen final
    print("\n🎯 ESTADO GENERAL:")
    if status["kinesis"]:
        print("✅ Ingesta de datos: Funcional")
    else:
        print("❌ Ingesta de datos: Falta Kinesis Stream")
        
    if status["dynamodb"]:
        print("✅ Almacenamiento: Funcional") 
    else:
        print("❌ Almacenamiento: Falta tabla DynamoDB")
        
    if status["lambda"]:
        print("✅ Procesamiento: Funcional")
    else:
        print("⚠️ Procesamiento: Lambda opcional para export")
    
    if status["kinesis"] and status["dynamodb"]:
        print("\n🏆 ¡Tu Data Warehouse está OPERATIVO!")
        print("🔄 Puedes ejecutar:")
        print("   python export_to_powerbi.py")
    else:
        print("\n⚠️ Tu Data Warehouse necesita configuración")

if __name__ == "__main__":
    verificar_estado_datawarehouse()