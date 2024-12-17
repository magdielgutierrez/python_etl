import os
import sqlite3
import pandas as pd
from datetime import date,datetime

def cnxn_sqlite():
    try:
        return sqlite3.connect("./db/sales_car.db")
    
    except sqlite3.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        print(e)
            
def save_system_logs(tipo, mensaje, detalle, estado):
    conn=cnxn_sqlite()
    cursor = conn.cursor() 
    try:
        fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('INSERT INTO system_flows_logs (fecha, tipo, mensaje, detalle, estado) VALUES (?, ?, ?, ?, ?)',(fecha, tipo, mensaje, detalle, estado))
        conn.commit()
        conn.close()
        print(f"\t\t|--->[LOGS]: Se registro el siguiente mensaje '{mensaje}'") 
    except sqlite3.Error as e:
        print(f"Error al insertar el log: {e}")
        
def read_raw_data(path):

    try:    
        return pd.read_csv(path, sep=',', encoding='utf-8')
    except FileNotFoundError:
        save_system_logs("Archivo", "Lectura de datos incorrecta", "El archivo no fue encontrado.", "ERROR")
        exit(1)
        
def save_raw_data(rawdf,table):
    try:
        rawdf.to_sql(table,cnxn_sqlite(), index=False, if_exists='append')
    except:
        rawdf.to_sql(table, cnxn_sqlite(), index=False, if_exists='replace')
    finally:
        print("\n--->[INFO]: Datos guardado correctamente")  
            
def executing_files_sql():           

    filesSQL = [f for f in os.listdir('./sql') if f.endswith('.sql')]
  
    cursor = cnxn_sqlite().cursor()
  
    for sql in filesSQL:
        print("\t\t|--->[SQL] - Ejecutando archivo ",sql )
        with open(f'./sql/{sql}', 'r') as myfile:
            data = myfile.read()       
            sqldata=data.split(';')
            for query in sqldata[:-1]:
                try:
                    cursor.execute(query)
                    print("\t\t|--->[INFO]: Secuencia SQL realizada correctamente")  
                except:
                    print(query)
                    break
            # Confirmar los cambios
            cnxn_sqlite().commit()

            # Cerrar la conexión
            cnxn_sqlite().close()
       
def validation_file(path,dfraw):
    
    # Rules #1 : Validando que el archivo ha sido modificado
    fec_modificado = datetime.fromtimestamp(os.path.getmtime(path)).strftime('%Y-%m-%d')
    if fec_modificado!=datetime.today().strftime('%Y-%m-%d'):
        save_system_logs("Archivo", "Archivo no esta actualizado", "Se mantiene un fecha diferente a la actual", "ERROR")
        exit(1)
    
    # Rules #2 : Validar campos obligatorios
    required_columns = ['year','make','model','trim','body','transmission','vin','state','condition','odometer','color','interior','seller','mmr','sellingprice','saledate']
    missing_columns = [col for col in required_columns if col not in dfraw.columns]
    if missing_columns:
        print(f"ERROR: Faltan las siguientes columnas obligatorias: {missing_columns}")
        save_system_logs("Estructura", "Archivo con estrcutura incorrecta", f"Faltan las siguientes columnas obligatorias: {missing_columns}", "ERROR")
        exit(1)
        
    # Rules #3 : Validar que se tenga datos
    if dfraw.empty:
        save_system_logs("Registros", "No contiene datos", "El archivo no contiene filas", "ERROR")
        exit(1)
        
    # Rules #4 : Validar cantidad de filas 
    if len(dfraw)>0:
        save_system_logs("Registros", "Cantidad de filas superior a ceros", f"{len(dfraw)} filas", "INFO")

                        
if __name__ == "__main__":
    
    ## Inicio
    _=os.system('cls')
    print("\n\t|--->[STEP]: INICIO") 

    c_year,c_month,c_day= date.today().year,date.today().month,date.today().day
    path='./files/car_prices.csv'
    
    ## EXTRACT: Leyendo data cruda 
    print("\n\t|--->[STEP]: EXTRACT: Leyendo data cruda") 
    dfraw=read_raw_data(path)
  
    # Validacion de calidad de datos
    validation_file(path,dfraw)
    
    # agregando particiones a data cruda
    dfraw['p_year'] = c_year
    dfraw['p_month'] = c_month
    dfraw['p_day'] = c_day
    
    ## LOAD: Guardando data en BD
    print("\n\t|--->[STEP]: LOAD: Guardando data en BD") 
    #save_raw_data(dfraw,'raw_car_prices')
    
    ## TRANSFORM: Filtrado y normalizado de la data
    print("\n\t|--->[STEP]: TRANSFORM: Filtrado y normalizado de la data") 
    executing_files_sql()
    
    ## Fin
    print("\n\t|--->[STEP]: FIN") 