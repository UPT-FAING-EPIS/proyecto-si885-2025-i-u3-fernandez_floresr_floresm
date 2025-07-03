import boto3
import pandas as pd
import json
from decimal import Decimal
from datetime import datetime
import schedule
import time
import os

class PowerBIAutoRefresh:
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
        self.table = self.dynamodb.Table('ofertas_trabajo')
        self.csv_file = 'ofertas_powerbi_live.csv'
        self.metadata_file = 'powerbi_metadata.json'
        
    def sync_data(self):
        """
        Sincroniza DynamoDB con CSV para Power BI
        """
        timestamp = datetime.now()
        print(f"🔄 {timestamp}: Sincronizando datos para Power BI...")
        
        try:
            # 1. Obtener datos de DynamoDB
            response = self.table.scan()
            items = response['Items']
            
            # Manejar paginación
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items.extend(response['Items'])
            
            if not items:
                print("❌ No hay datos en DynamoDB")
                return False
            
            # 2. Convertir a DataFrame
            def decimal_to_float(obj):
                if isinstance(obj, Decimal):
                    return float(obj)
                raise TypeError
            
            items_json = json.loads(json.dumps(items, default=decimal_to_float))
            df = pd.DataFrame(items_json)
            
            # 3. Limpiar datos para Power BI
            list_columns = [
                'Lenguajes_Lista', 'Frameworks_Lista', 'Bases_Datos_Lista', 
                'Herramientas_Lista', 'Conocimientos_Adicionales_Lista'
            ]
            
            for col in list_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: 
                        ', '.join(x) if isinstance(x, list) and x else 'No especificado'
                    )
            
            # Rellenar valores nulos
            df = df.fillna('No especificado')
            
            # 4. Agregar metadatos de actualización
            df['ultima_actualizacion'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            df['version_datos'] = timestamp.strftime('%Y%m%d_%H%M')
            
            # 5. Detectar cambios
            cambios_detectados = True
            if os.path.exists(self.csv_file):
                df_anterior = pd.read_csv(self.csv_file)
                if len(df) == len(df_anterior):
                    # Comparar contenido (sin timestamp)
                    df_compare = df.drop(['ultima_actualizacion', 'version_datos'], axis=1, errors='ignore')
                    df_anterior_compare = df_anterior.drop(['ultima_actualizacion', 'version_datos'], axis=1, errors='ignore')
                    if df_compare.equals(df_anterior_compare):
                        cambios_detectados = False
            
            # 6. Guardar solo si hay cambios
            if cambios_detectados:
                df.to_csv(self.csv_file, index=False, encoding='utf-8-sig')
                
                # Crear metadata
                metadata = {
                    'ultima_sync': timestamp.isoformat(),
                    'total_registros': len(df),
                    'archivo_csv': self.csv_file,
                    'cambios_detectados': True,
                    'version': timestamp.strftime('%Y%m%d_%H%M'),
                    'columnas': list(df.columns),
                    'status': 'OK'
                }
                
                with open(self.metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False)
                
                print(f"✅ Datos actualizados: {len(df)} registros")
                print(f"📊 Archivo: {self.csv_file}")
                print(f"🔄 Power BI detectará los cambios automáticamente")
                
            else:
                print("ℹ️ No hay cambios en los datos - sin actualización")
            
            return True
            
        except Exception as e:
            print(f"❌ Error en sincronización: {e}")
            
            # Metadata de error
            metadata = {
                'ultima_sync': timestamp.isoformat(),
                'status': 'ERROR',
                'error': str(e),
                'cambios_detectados': False
            }
            
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            return False
    
    def iniciar_monitor(self, intervalo_minutos=30):
        """
        Inicia el monitor de auto-refresh
        """
        print("🚀 INICIANDO AUTO-REFRESH PARA POWER BI")
        print("=" * 50)
        print(f"📊 Archivo CSV: {self.csv_file}")
        print(f"⏰ Intervalo: cada {intervalo_minutos} minutos")
        print(f"🔄 Power BI: configurar refresh automático en el archivo CSV")
        print("📋 Presiona Ctrl+C para detener")
        print("=" * 50)
        
        # Sync inicial
        self.sync_data()
        
        # Programar actualizaciones
        schedule.every(intervalo_minutos).minutes.do(self.sync_data)
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        except KeyboardInterrupt:
            print("\n🛑 Auto-refresh detenido")
            print("💾 Última sincronización preservada")

def main():
    import sys
    
    refresh_manager = PowerBIAutoRefresh()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--once':
            # Ejecutar una sola vez
            refresh_manager.sync_data()
        elif sys.argv[1] == '--monitor':
            # Monitor continuo (default 30 min)
            intervalo = int(sys.argv[2]) if len(sys.argv) > 2 else 30
            refresh_manager.iniciar_monitor(intervalo)
    else:
        # Una sola ejecución por default
        refresh_manager.sync_data()

if __name__ == "__main__":
    main()