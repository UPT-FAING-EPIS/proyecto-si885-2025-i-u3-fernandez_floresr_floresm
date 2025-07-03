import boto3
import json
import pandas as pd
from datetime import datetime
import time
import uuid
import os

# Configurar cliente Kinesis para us-east-2
kinesis = boto3.client('kinesis', region_name='us-east-2')
nombre_stream = 'streamOfertas'

def limpiar_lista_para_dashboard(valor):
    """
    Limpia listas SOLO para mejorar dashboards - usando TUS datos
    """
    if pd.isna(valor) or valor == '':
        return []
    
    if isinstance(valor, str):
        # Separar por comas y limpiar cada elemento
        items = []
        for item in valor.split(','):
            item_limpio = item.strip()  # Quitar espacios
            if item_limpio:  # Solo si no está vacío
                # Capitalizar primera letra para consistencia
                items.append(item_limpio.title())
        
        # Remover duplicados manteniendo orden
        items_unicos = []
        for item in items:
            if item not in items_unicos:
                items_unicos.append(item)
        
        return items_unicos
    
    return []

def cargar_ofertas_a_kinesis():
    """
    Carga las ofertas de trabajo desde ofertas_trabajo.csv a Kinesis
    """
    print("=== INICIANDO CARGA DE OFERTAS A KINESIS ===")
    print("🌍 Región: us-east-2")
    
    archivo_ofertas = 'ofertas_trabajo.csv'
    if not os.path.exists(archivo_ofertas):
        print(f"❌ Error: No se encuentra el archivo {archivo_ofertas}")
        return False
    
    # Cargar archivo de ofertas
    try:
        df = pd.read_csv(archivo_ofertas, encoding='utf-8-sig')
        print(f"✅ Archivo cargado: {len(df)} ofertas encontradas")
    except Exception as e:
        print(f"❌ Error cargando archivo: {e}")
        return False

    ofertas_enviadas = 0
    errores = 0

    print("🚀 Iniciando envío de datos a Kinesis us-east-2...")

    for index, row in df.iterrows():
        try:
            salario_monto_str = str(row.get('Salario_Monto', '0')).replace('$', '').replace(',', '').strip()
            try:
                salario_monto = float(salario_monto_str)
            except ValueError:
                salario_monto = 0.0

            # Función para convertir valores de forma segura
            def safe_int(value, default=0):
                if pd.isna(value) or str(value).lower() in ['no disponible', 'n/a', '', 'nan', 'none']:
                    return default
                try:
                    return int(float(str(value)))
                except (ValueError, TypeError):
                    return default

            record = {
                'ID_Oferta': str(row.get('ID_Oferta', '')),
                'Titulo_Oferta': str(row.get('Titulo_Oferta', '')),
                'Ciudad': str(row.get('Ciudad', '')),
                'Region_Departamento': str(row.get('Region_Departamento', '')),
                'Fecha_Publicacion': str(row.get('Fecha_Publicacion', '')),
                'Tipo_Contrato': str(row.get('Tipo_Contrato', '')),
                'Tipo_Jornada': str(row.get('Tipo_Jornada', '')),
                'Modalidad_Trabajo': str(row.get('Modalidad_Trabajo', '')),
                'Salario_Monto': salario_monto,
                'Salario_Moneda': str(row.get('Salario_Moneda', '')),
                'Salario_Tipo_Pago': str(row.get('Salario_Tipo_Pago', '')),
                
                # 🔧 CAMBIO: Limpiar para dashboards profesionales
                'Lenguajes_Lista': limpiar_lista_para_dashboard(row.get('Lenguajes_Lista', '')),
                'Frameworks_Lista': limpiar_lista_para_dashboard(row.get('Frameworks_Lista', '')),
                'Bases_Datos_Lista': limpiar_lista_para_dashboard(row.get('Bases_Datos_Lista', '')),
                'Herramientas_Lista': limpiar_lista_para_dashboard(row.get('Herramientas_Lista', '')),
                'Conocimientos_Adicionales_Lista': limpiar_lista_para_dashboard(row.get('Conocimientos_Adicionales_Lista', '')),
                
                'Nivel_Ingles': str(row.get('Nivel_Ingles', '')),
                'Nivel_Educacion': str(row.get('Nivel_Educacion', '')),
                'Anos_Experiencia': safe_int(row.get('Anos_Experiencia', 0)),
                'Edad_Minima': safe_int(row.get('Edad_Minima', 0)),
                'Edad_Maxima': safe_int(row.get('Edad_Maxima', 0)),
                'Categoria_Puesto': str(row.get('Categoria_Puesto', '')),
                'Nombre_Empresa': str(row.get('Nombre_Empresa', '')),
                'Contenido_Descripcion_Empresa': str(row.get('Contenido_Descripcion_Empresa', '')),
                'Enlace_Oferta': str(row.get('Enlace_Oferta', '')),
                'Contenido_Descripcion_Oferta': str(row.get('Contenido_Descripcion_Oferta', ''))
            }
            
            # Mostrar progreso cada 50 registros
            if (index + 1) % 50 == 0:
                print(f"📤 Enviando registro {index + 1}/{len(df)}: {record['Titulo_Oferta'][:30]}...")

            # Enviar a Kinesis
            response = kinesis.put_record(
                StreamName=nombre_stream,
                Data=json.dumps(record, ensure_ascii=False),
                PartitionKey=str(uuid.uuid4())
            )

            ofertas_enviadas += 1
            time.sleep(0.1)  # Pausa pequeña para evitar throttling

        except Exception as e:
            errores += 1
            if errores <= 5:  # Solo mostrar primeros 5 errores
                print(f"❌ Error procesando registro {index + 1}: {e}")
            continue

    print(f"\n=== RESUMEN DE CARGA ===")
    print(f"✅ Ofertas enviadas exitosamente: {ofertas_enviadas}")
    print(f"❌ Errores encontrados: {errores}")
    print(f"📊 Total procesado: {len(df)}")
    print(f"🎯 Tasa de éxito: {(ofertas_enviadas/len(df)*100):.1f}%")

    return ofertas_enviadas > 0

if __name__ == "__main__":
    print("🚀 INICIANDO CARGA DE OFERTAS DE TRABAJO A AWS KINESIS")
    print("=" * 60)
    print("🌍 Región configurada: us-east-2")

    if cargar_ofertas_a_kinesis():
        print("\n🎉 ¡Carga completada exitosamente!")
        print("⏳ Espera 2-3 minutos para que Lambda procese los datos")
        print("📊 Ve a DynamoDB en us-east-2 para verificar los datos")
    else:
        print("\n❌ La carga falló. Revisa los errores anteriores.")