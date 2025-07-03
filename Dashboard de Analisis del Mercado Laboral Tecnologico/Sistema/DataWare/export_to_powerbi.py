import boto3
import pandas as pd
import json
from decimal import Decimal
from datetime import datetime

def exportar_para_powerbi():
    """
    Exporta los datos del Data Warehouse para Power BI con listas normalizadas
    """
    print("📊 EXPORTANDO DATA WAREHOUSE PARA POWER BI")
    print("=" * 50)
    
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('ofertas_trabajo')
    
    try:
        # Obtener todos los datos
        response = table.scan()
        items = response['Items']
        
        # Manejar paginación si hay muchos registros
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        print(f"✅ {len(items)} registros obtenidos del Data Warehouse")
        
        if not items:
            print("❌ No hay datos en la tabla")
            return False
        
        # Convertir Decimals a float para Power BI
        def decimal_default(obj):
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError
        
        # Convertir a formato JSON y luego a DataFrame
        items_json = json.loads(json.dumps(items, default=decimal_default))
        df = pd.DataFrame(items_json)
        
        # ✅ MEJORADO: Preparar datos para Power BI con análisis avanzado
        if not df.empty:
            print("🔧 Procesando listas para análisis en Power BI...")
            
            # Columnas de listas tecnológicas
            list_columns = ['Lenguajes_Lista', 'Frameworks_Lista', 'Bases_Datos_Lista', 
                           'Herramientas_Lista', 'Conocimientos_Adicionales_Lista']
            
            # Procesar cada columna de lista
            for col in list_columns:
                if col in df.columns:
                    print(f"   📋 Procesando: {col}")
                    
                    # Convertir listas a string separado por " | " (mejor para Power BI)
                    df[col] = df[col].apply(lambda x: 
                        ' | '.join(x) if isinstance(x, list) and x else 
                        'No especificado'
                    )
                    
                    # ✅ NUEVO: Crear columnas adicionales para análisis
                    if col == 'Lenguajes_Lista':
                        # Contar cantidad de lenguajes
                        df['Total_Lenguajes'] = df[col].apply(
                            lambda x: len(x.split(' | ')) if x != 'No especificado' else 0
                        )
                        
                        # Crear columnas boolean para lenguajes populares
                        lenguajes_populares = ['Python', 'JavaScript', 'Java', 'C#', 'React', 'Angular']
                        for lenguaje in lenguajes_populares:
                            df[f'Usa_{lenguaje}'] = df[col].apply(
                                lambda x: 'Sí' if lenguaje in x else 'No'
                            )
                    
                    elif col == 'Frameworks_Lista':
                        df['Total_Frameworks'] = df[col].apply(
                            lambda x: len(x.split(' | ')) if x != 'No especificado' else 0
                        )
                    
                    elif col == 'Bases_Datos_Lista':
                        df['Total_BD'] = df[col].apply(
                            lambda x: len(x.split(' | ')) if x != 'No especificado' else 0
                        )
            
            # ✅ NUEVO: Crear categorías de salario para análisis
            if 'Salario_Monto' in df.columns:
                print("   💰 Creando rangos de salario...")
                def categorizar_salario(salario):
                    if pd.isna(salario) or salario == 0:
                        return 'No especificado'
                    elif salario < 30000:
                        return 'Hasta $30K'
                    elif salario < 50000:
                        return '$30K - $50K'
                    elif salario < 80000:
                        return '$50K - $80K'
                    elif salario < 120000:
                        return '$80K - $120K'
                    else:
                        return 'Más de $120K'
                
                df['Rango_Salario'] = df['Salario_Monto'].apply(categorizar_salario)
            
            # ✅ NUEVO: Normalizar modalidades de trabajo
            if 'Modalidad_Trabajo' in df.columns:
                print("   🏢 Normalizando modalidades...")
                df['Modalidad_Trabajo'] = df['Modalidad_Trabajo'].apply(
                    lambda x: str(x).strip().title() if pd.notna(x) else 'No especificado'
                )
            
            # Rellenar valores vacíos
            df = df.fillna('No especificado')
            
            # ✅ NUEVO: Agregar metadatos para Power BI
            df['Fecha_Exportacion'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['Version_Dataset'] = datetime.now().strftime('%Y%m%d_%H%M')
            
            # Exportar a CSV optimizado para Power BI
            archivo_powerbi = 'powerbi_ofertas_trabajo.csv'
            df.to_csv(archivo_powerbi, index=False, encoding='utf-8-sig')
            
            print(f"✅ Datos exportados a: {archivo_powerbi}")
            print(f"📈 {len(df)} registros listos para Power BI")
            print(f"📊 Columnas disponibles: {len(df.columns)}")
            
            # ✅ NUEVO: Mostrar columnas por categoría
            print(f"\n📋 COLUMNAS ORGANIZADAS PARA POWER BI:")
            
            print("   🏢 INFORMACIÓN BÁSICA:")
            basicas = ['ID_Oferta', 'Titulo_Oferta', 'Nombre_Empresa', 'Ciudad', 'Region_Departamento']
            for col in basicas:
                if col in df.columns:
                    print(f"      • {col}")
            
            print("   💼 CONDICIONES LABORALES:")
            laborales = ['Tipo_Contrato', 'Tipo_Jornada', 'Modalidad_Trabajo', 'Salario_Monto', 'Rango_Salario']
            for col in laborales:
                if col in df.columns:
                    print(f"      • {col}")
            
            print("   🛠️ TECNOLOGÍAS:")
            tecnologias = ['Lenguajes_Lista', 'Frameworks_Lista', 'Bases_Datos_Lista', 'Herramientas_Lista']
            for col in tecnologias:
                if col in df.columns:
                    print(f"      • {col}")
            
            print("   📊 MÉTRICAS CALCULADAS:")
            metricas = ['Total_Lenguajes', 'Total_Frameworks', 'Total_BD']
            for col in metricas:
                if col in df.columns:
                    print(f"      • {col}")
            
            print("   ✅ FILTROS BOOLEAN:")
            filtros = [col for col in df.columns if col.startswith('Usa_')]
            for col in filtros:
                print(f"      • {col}")
            
            # Mostrar estadísticas del dataset
            print(f"\n📈 ESTADÍSTICAS DEL DATASET:")
            if 'Nombre_Empresa' in df.columns:
                print(f"   🏢 Empresas únicas: {df['Nombre_Empresa'].nunique()}")
            if 'Ciudad' in df.columns:
                print(f"   🌍 Ciudades únicas: {df['Ciudad'].nunique()}")
            if 'Categoria_Puesto' in df.columns:
                print(f"   💼 Categorías de puesto: {df['Categoria_Puesto'].nunique()}")
            if 'Salario_Monto' in df.columns:
                salarios = pd.to_numeric(df['Salario_Monto'], errors='coerce')
                salarios = salarios.replace(0, None).dropna()
                if not salarios.empty:
                    print(f"   💰 Salario promedio: ${salarios.mean():,.0f}")
                    print(f"   💰 Salario mediano: ${salarios.median():,.0f}")
            
            # ✅ NUEVO: Mostrar top tecnologías
            if 'Lenguajes_Lista' in df.columns:
                print(f"\n🏆 TOP TECNOLOGÍAS MÁS DEMANDADAS:")
                # Contar menciones de cada tecnología
                todas_tecnologias = []
                for lista in df['Lenguajes_Lista']:
                    if lista != 'No especificado':
                        tecnologias = [tech.strip() for tech in lista.split(' | ')]
                        todas_tecnologias.extend(tecnologias)
                
                if todas_tecnologias:
                    tech_counts = pd.Series(todas_tecnologias).value_counts().head(5)
                    for i, (tech, count) in enumerate(tech_counts.items(), 1):
                        porcentaje = (count / len(df)) * 100
                        print(f"   {i}. {tech}: {count} ofertas ({porcentaje:.1f}%)")
            
            return True
        else:
            print("❌ DataFrame vacío después de la conversión")
            return False
            
    except Exception as e:
        print(f"❌ Error exportando: {e}")
        print(f"🔍 Detalle del error: {type(e).__name__}")
        return False

if __name__ == "__main__":
    if exportar_para_powerbi():
        print("\n🎉 ¡EXPORT COMPLETADO!")
        print("📄 Archivo generado: powerbi_ofertas_trabajo.csv")
        print("\n🔄 PRÓXIMOS PASOS EN POWER BI:")
        print("   1. 📂 Abrir Power BI Desktop")
        print("   2. 🔗 Obtener datos → Texto/CSV") 
        print("   3. 📁 Seleccionar 'powerbi_ofertas_trabajo.csv'")
        print("   4. ✅ Verificar tipos de datos automáticos")
        print("   5. 🎨 Crear dashboards y reportes")
        print("\n💡 SUGERENCIAS PARA TU DASHBOARD:")
        print("   📊 Gráfico de barras: Top lenguajes más demandados")
        print("   🥧 Gráfico circular: Distribución por modalidad")
        print("   🗺️ Mapa: Ofertas por ciudad")
        print("   📈 Línea de tiempo: Tendencias de publicación")
        print("   💰 KPI: Salario promedio por tecnología")
    else:
        print("\n❌ Export falló. Verifica la conexión a AWS.")