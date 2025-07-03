import boto3
import json
from datetime import datetime

def limpiar_tabla_dynamodb():
    """
    Limpia completamente la tabla DynamoDB
    """
    print("🗑️ LIMPIANDO TABLA DYNAMODB")
    print("=" * 50)
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('ofertas_trabajo')
    
    try:
        # 1. Verificar tabla existe
        response = table.table_status
        print(f"✅ Tabla encontrada: {table.name}")
        print(f"📊 Estado: {response}")
        
        # 2. Contar registros actuales
        scan = table.scan(Select='COUNT')
        total_items = scan['Count']
        
        # Manejar paginación para conteo exacto
        while 'LastEvaluatedKey' in scan:
            scan = table.scan(
                Select='COUNT',
                ExclusiveStartKey=scan['LastEvaluatedKey']
            )
            total_items += scan['Count']
        
        print(f"📋 Registros actuales: {total_items}")
        
        if total_items == 0:
            print("ℹ️ La tabla ya está vacía")
            return True
        
        # 3. Confirmar eliminación
        print(f"\n⚠️ ATENCIÓN: Se eliminarán {total_items} registros")
        confirmacion = input("¿Estás seguro? Escribe 'ELIMINAR' para confirmar: ")
        
        if confirmacion != 'ELIMINAR':
            print("❌ Operación cancelada")
            return False
        
        # 4. Obtener y eliminar todos los items
        print("🔄 Eliminando registros...")
        
        # Scan para obtener claves primarias
        scan = table.scan(ProjectionExpression='ID_Oferta')
        items_to_delete = scan['Items']
        
        # Manejar paginación
        while 'LastEvaluatedKey' in scan:
            scan = table.scan(
                ProjectionExpression='ID_Oferta',
                ExclusiveStartKey=scan['LastEvaluatedKey']
            )
            items_to_delete.extend(scan['Items'])
        
        print(f"📋 Items a eliminar: {len(items_to_delete)}")
        
        # Eliminar en lotes (más eficiente)
        eliminados = 0
        errores = 0
        
        # Procesar en lotes de 25 (límite de DynamoDB)
        for i in range(0, len(items_to_delete), 25):
            lote = items_to_delete[i:i+25]
            
            try:
                with table.batch_writer() as batch:
                    for item in lote:
                        batch.delete_item(Key={'ID_Oferta': item['ID_Oferta']})
                        eliminados += 1
                
                # Mostrar progreso
                if eliminados % 100 == 0:
                    print(f"   🗑️ Eliminados: {eliminados}/{len(items_to_delete)}")
                    
            except Exception as e:
                errores += 1
                print(f"❌ Error en lote {i//25 + 1}: {e}")
        
        print(f"\n✅ LIMPIEZA COMPLETADA:")
        print(f"   🗑️ Eliminados: {eliminados}")
        print(f"   ❌ Errores: {errores}")
        print(f"   📊 Tasa de éxito: {(eliminados/len(items_to_delete)*100):.1f}%")
        
        # 5. Verificar tabla vacía
        scan_final = table.scan(Select='COUNT')
        print(f"   📋 Registros restantes: {scan_final['Count']}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error limpiando tabla: {e}")
        return False

def respaldar_antes_limpiar():
    """
    Crear respaldo antes de limpiar (opcional)
    """
    print("💾 CREANDO RESPALDO ANTES DE LIMPIAR")
    print("=" * 50)
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        table = dynamodb.Table('ofertas_trabajo')
        
        # Scan todos los datos
        response = table.scan()
        items = response['Items']
        
        # Manejar paginación
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        if not items:
            print("ℹ️ No hay datos para respaldar")
            return True
        
        # Convertir Decimal a float para JSON
        def decimal_default(obj):
            if isinstance(obj, boto3.dynamodb.types.TypeDeserializer().deserialize.__self__.__class__):
                return float(obj)
            raise TypeError
        
        # Guardar respaldo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archivo_respaldo = f'respaldo_ofertas_{timestamp}.json'
        
        with open(archivo_respaldo, 'w', encoding='utf-8') as f:
            json.dump(items, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"✅ Respaldo creado: {archivo_respaldo}")
        print(f"📊 Registros respaldados: {len(items)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creando respaldo: {e}")
        return False

def menu_limpieza():
    """
    Menú interactivo para limpieza
    """
    print("🔧 GESTIÓN DE TABLA DYNAMODB")
    print("=" * 50)
    print("1. 💾 Crear respaldo y limpiar tabla")
    print("2. 🗑️ Limpiar tabla (sin respaldo)")
    print("3. 📊 Solo mostrar estadísticas actuales")
    print("4. ❌ Cancelar")
    
    opcion = input("\nSelecciona una opción (1-4): ").strip()
    
    if opcion == '1':
        print("\n💾 Creando respaldo primero...")
        if respaldar_antes_limpiar():
            print("\n🗑️ Procediendo a limpiar...")
            return limpiar_tabla_dynamodb()
        else:
            print("❌ Error en respaldo, operación cancelada")
            return False
            
    elif opcion == '2':
        return limpiar_tabla_dynamodb()
        
    elif opcion == '3':
        mostrar_estadisticas_tabla()
        return True
        
    elif opcion == '4':
        print("❌ Operación cancelada")
        return False
        
    else:
        print("❌ Opción inválida")
        return False

def mostrar_estadisticas_tabla():
    """
    Muestra estadísticas de la tabla actual
    """
    print("📊 ESTADÍSTICAS TABLA DYNAMODB")
    print("=" * 50)
    
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        table = dynamodb.Table('ofertas_trabajo')
        
        # Información general
        response = table.scan(Select='COUNT')
        total_items = response['Count']
        
        # Obtener muestra de datos
        sample_scan = table.scan(Limit=5)
        sample_items = sample_scan['Items']
        
        print(f"📋 Total registros: {total_items}")
        print(f"📊 Estado tabla: {table.table_status}")
        
        if sample_items:
            print(f"\n📄 Muestra de registros:")
            for i, item in enumerate(sample_items[:3], 1):
                print(f"   {i}. {item.get('Titulo_Oferta', 'Sin título')[:50]}...")
                print(f"      ID: {item.get('ID_Oferta', 'Sin ID')}")
                print(f"      Empresa: {item.get('Nombre_Empresa', 'Sin empresa')}")
                print()
        
        return True
        
    except Exception as e:
        print(f"❌ Error obteniendo estadísticas: {e}")
        return False

if __name__ == "__main__":
    menu_limpieza()