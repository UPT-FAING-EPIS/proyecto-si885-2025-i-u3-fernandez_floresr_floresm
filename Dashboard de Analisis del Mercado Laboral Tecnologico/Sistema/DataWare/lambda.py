import base64
import json
import boto3
import decimal
from datetime import datetime

dynamo = boto3.resource('dynamodb', region_name='us-east-2')
tabla_ofertas = dynamo.Table('ofertas_trabajo')

def lambda_handler(event, context):
    for record in event['Records']:
        try:
            # Decodificar y deserializar el registro de Kinesis
            payload = base64.b64decode(record['kinesis']['data'])
            item = json.loads(payload)
            
            # Preparar el ítem para DynamoDB (convertir decimales si es necesario)
            dynamo_item = {
                'ID_Oferta': item.get('ID_Oferta', ''),
                'Titulo_Oferta': item.get('Titulo_Oferta', ''),
                'Ciudad': item.get('Ciudad', ''),
                'Region_Departamento': item.get('Region_Departamento', ''),
                'Fecha_Publicacion': item.get('Fecha_Publicacion', ''),
                'Tipo_Contrato': item.get('Tipo_Contrato', ''),
                'Tipo_Jornada': item.get('Tipo_Jornada', ''),
                'Modalidad_Trabajo': item.get('Modalidad_Trabajo', ''),
                'Salario_Monto': decimal.Decimal(str(item.get('Salario_Monto', 0))),
                'Salario_Moneda': item.get('Salario_Moneda', ''),
                'Salario_Tipo_Pago': item.get('Salario_Tipo_Pago', ''),
                'Lenguajes_Lista': item.get('Lenguajes_Lista', []),
                'Frameworks_Lista': item.get('Frameworks_Lista', []),
                'Bases_Datos_Lista': item.get('Bases_Datos_Lista', []),
                'Herramientas_Lista': item.get('Herramientas_Lista', []),
                'Nivel_Ingles': item.get('Nivel_Ingles', ''),
                'Nivel_Educacion': item.get('Nivel_Educacion', ''),
                'Anos_Experiencia': int(item.get('Anos_Experiencia', 0)),
                'Conocimientos_Adicionales_Lista': item.get('Conocimientos_Adicionales_Lista', []),
                'Edad_Minima': int(item.get('Edad_Minima', 0)),
                'Edad_Maxima': int(item.get('Edad_Maxima', 0)),
                'Categoria_Puesto': item.get('Categoria_Puesto', ''),
                'Nombre_Empresa': item.get('Nombre_Empresa', ''),
                'Contenido_Descripcion_Empresa': item.get('Contenido_Descripcion_Empresa', ''),
                'Enlace_Oferta': item.get('Enlace_Oferta', ''),
                'Contenido_Descripcion_Oferta': item.get('Contenido_Descripcion_Oferta', ''),
                'fecha_procesamiento': datetime.now().isoformat()
            }
            
            tabla_ofertas.put_item(Item=dynamo_item)
            print(f"✅ Oferta almacenada: {dynamo_item['ID_Oferta']}")
        except Exception as e:
            print(f"❌ Error procesando registro: {str(e)}")
    return {'statusCode': 200, 'body': 'Procesamiento completado'}