# Importar librerias necesarias
import os
import sys
import pandas as pd
from Functions import *
from Connections import *
sys.path.append(os.path.join(".."))
from Imports import *

#---- Docuento basado en funciones para trabajar archivos excel y csv y comprimidos ----# 
#-- Función inicial scan_folder
#-- Función segudaria Read_files_path
#-- Función terciaria check_and_add
#-- Funciónes de tratamiento y cargue la informacion a las tablas: toSqlTxt,toSqlExcel
#-- Funciónes de tratamiento de datos en columnas: convertir_fecha,insertar_raya_al_piso 
script_dir = os.path.dirname(os.path.abspath(__file__))
log_file_path = os.path.join(script_dir, '..', 'log.yml')
log_file_path1 = os.path.join(script_dir, '..', 'LoadedFiles')

with open(log_file_path) as f: #/root/PROCESOS/BASESCLAROCOBRANZAPRUEBAS/src/log.yml
    cnf = yaml.safe_load(f)
    logging.config.dictConfig(cnf)

# Variables globales para el proceso
inicio  = time.time()
anio    = datetime.datetime.now().strftime("%Y")
mes     = datetime.datetime.now().strftime("%m")
dia     = datetime.datetime.now().strftime("%d")
hora    = datetime.datetime.now().strftime("%H:%M")
fecha   = datetime.datetime.now().strftime("%Y-%m-%d")
ayer    = datetime.datetime.now() - datetime.timedelta(days=1)
ayer    = ayer.strftime("%d") 

# Función segundaria del archivo
def Read_files_path(path_,nombre_tabla,nombre_archivo):
    try: 
        print("BUSCANDO ARCHIVOS")
        archivos_coincidentes =[]
        print(nombre_archivo)
        archivos_total = os.listdir(path_) # Listar los archivos del direcctorio 
        print(archivos_total)
        for a in nombre_archivo: # Iterar loa archivos del directrio 
            archivos = [valor for valor in archivos_total if f"{a}" in valor] # Selecionar los archivos deseados
            archivos_coincidentes.extend(archivos) # agregar el archivo a la lista 
        print(f"cantidad archivos: {archivos_coincidentes}\n")
    except Exception as e:
        print("error")
        logging.getLogger("user").exception(e)
        raise 
    with open(f"{log_file_path1}//{nombre_tabla}.log", "r") as f: # //root//PROCESOS//BASESCLAROCOBRANZAPRUEBAS//src//LoadedFiles//
        loaded_files = f.read().splitlines()  # todo el listado 
        # Obtener la lista de archivos con fechas de modificación
        archivos_con_fecha = [f"{datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path_, archivo))).strftime('%Y-%m-%d %H:%M:%S')} - {archivo}" for archivo in archivos_coincidentes]
        # Comparación de los archivos ya cargados y los archivos para cargar así cargar las diferencias
        different_files = set(archivos_con_fecha).difference(loaded_files)   
    if len(different_files) == 0:
        logging.getLogger("user").debug("No hay archivos para cargar")
        print(different_files)
    return different_files # Archivos a cargar

def insertar_raya_al_piso(cadena): #Funcion que inserta raya al piso cuando el encabezado es CamelCase
        nueva_cadena=""
        try:
            if cadena.isupper() or cadena.islower() or ("_" in cadena):
                nueva_cadena=cadena
            else:
                i=0
                for letra in cadena:
                    if letra.isupper() and i!=0:
                        nueva_cadena =nueva_cadena + "_" + letra
                    else:
                        nueva_cadena = nueva_cadena + letra
                    i+=1
            return nueva_cadena
        except:
            return cadena

def convertir_fecha(fecha):
    #print("Hola, ingresé: ",fecha)
    formatos = ["%Y-%m-%d %I:%M:%S", "%d/%m/%Y %I:%M:%S", "%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m-%d-%y", "%d/%m/%Y:%H:%M:%S", "%d-%m-%Y %H:%M:%S", "%d/%m/%Y 0:00:00", "%d/%m/%Y 00:00:00", "%d/%m/%Y %H:%M:%S","%Y-%m-%d %H:%M:%S","%d/%m/%Y %I:%M:%S %p"]
    for formato in formatos:
        try:
            fecha = pd.to_datetime(fecha, format=formato)
            return str(fecha)
        except Exception as e:
            #print("ERROR: con la fecha: ",e)
            pass
    
        # Si no coincide con ninguno de los formatos, intenta manejar el caso especial de fechas cortas
        try:
            if len(str(fecha)) <= 5:
                x = datetime(1900, 1, 1)
                fecha = x + pd.to_timedelta(int(fecha) - 2, unit='D')
                return str(fecha)
        except ValueError:
                pass
    
                # Si no puede convertir la fecha, devuelve un mensaje indicando el problema
                return "No se pudo convertir la fecha"
            
def toSqlTxt(path,nombre_tabla,file_, dic_fechas, dic_formatos, separador, columnas_tabla,cruze):
    logging.getLogger("user").info(f"cargando archivo de texto")
    logging.info('Lectura archivo')
    print(path)
    print(file_)
    if len(columnas_tabla)==0:
        df = dd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False)
    else:
        df = dd.read_csv(path+"/"+file_[22:],sep = separador,dtype=str, index_col=False, names=columnas_tabla, encoding='latin-1')
    print("dataframe juan: ",df)
    fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
    tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(" ", "_", regex=True)
    df.columns = df.columns.str.translate(tabla_reemplazo)
    df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
    df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
    df.columns = df.columns.str.upper()
    connection, cdn_connection, engine, bbdd_or = mysql_connection()
    print("Cantidad antes de filtrar: ", len(df))
    try:
        Cruze = cruze[0]
        if Cruze == "1":
            consulta = text("SELECT DISTINCT CUENTA_FS FROM tb_asignacion_con_fs_potencial_pyme_bloqueo WHERE MONTH(FEC_ASIGNA) = MONTH(CURDATE());")
            resultado = connection.execute(consulta)  # Utiliza 'connection' en lugar de 'engine'
            dfasg = pd.DataFrame(resultado)
            # dfasg = dd.from_pandas(resultado, npartitions=1)
            # print(cruze[1])
            df = df[df[cruze[1]].isin(dfasg["CUENTA_FS"])]
    except:
        print("No tirne cruce.")
    print("Cantidad despues: ",len(df))  
    print(dic_fechas)
    print(df.columns)
    for fecha_mod in dic_fechas:
                    if fecha_mod in df.columns:
                        df[fecha_mod]=df[fecha_mod].apply(convertir_fecha, meta=(f"{fecha_mod}", "str")) # 

    df['FILE_DATE'] = fecha                                     
    df['FILE_NAME'] = file_[22:]
    df['FILE_YEAR'] = df['FILE_DATE'].dt.year
    df['FILE_MONTH']  = df['FILE_DATE'].dt.month
    print(dic_formatos)
    for formato in dic_formatos:
        if formato in df.columns:
            df[formato] = df[formato].str.replace(",", ".", regex=True)
            df[formato] = df[formato].str.replace("[^0-9-.]", "", regex=True)
    
    try:
        tabla = Table(f"tb_{nombre_tabla}", MetaData(), autoload_with = engine)
        nombre_columnas_nuevas = [c.name for c in tabla.c]
        diffCols = df.columns.difference(nombre_columnas_nuevas)
        listCols = list(diffCols)
        if len(listCols)!=0:
            for i in range(len(listCols)):
                logging.getLogger("user").info(f"Columna {i} agregada.")
                connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
        tabla = Table(f"tb_{nombre_tabla}", MetaData(), autoload_with = engine)
        dask.config.set(scheduler="processes")
        print("Llegueee...")
        #df.repartition(npartitions=10)
        columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
        tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
        tmp.drop(bind = engine,checkfirst=True)
        tmp.create(bind = engine)
        logging.getLogger("user").info('Creacion Tabla temporal')
        # Cargue del DataFrame de Dask en la base de datos MySQL.
        if cruze[2] == "1":
            connection.execute(text(f"TRUNCATE {tabla.name} ;"))
            print("Ejecucion Trucate: ", tabla.name)     
        df.to_sql(tmp.name, cdn_connection, if_exists='append',index=False) #,parallel=True
        connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
        tmp.drop(bind = engine,checkfirst=True)
        logging.getLogger("user").info('Insercion Tabla destino DW')
    except Exception as e:
        print(f"ERROR {e}")
        logging.getLogger("user").exception(e)
        raise

def toSqlExcel(path,nombre_tabla,file_, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion,columnas_tabla , columnas_sin_espacio):
    try:
        logging.info('Lectura archivo')
        excel_file = pd.ExcelFile(path+"/"+file_[22:])
        hojas_documento = dic_hojas
        fechaHoraActual=datetime.datetime.now()

        for sheet in hojas_documento:
            if sheet == "None":
                sheet=0
            try:
                if len(columnas_tabla)==0:
                    df = pd.read_excel(path+"/"+file_[22:], sheet_name = sheet, dtype=str)
                else:
                    df = pd.read_excel(path+"/"+file_[22:],sheet_name = sheet, dtype=str, names=columnas_tabla)
                if sheet == "CUENTAS" and "Exclusion_Gestion_Cuentas" in file_:
                    df1= df[['CUENTA', 'FECHA', 'MES', 'SDS']]
                    df2 = df[['CUENTA.1', 'FECHA.1', 'MES.1']]
                    print(df2)
                    df2.rename(columns={'CUENTA.1':'CUENTA','FECHA.1':'FECHA',  'MES.1': 'MES'}, inplace=True)
                    df= pd.concat([df1, df2], axis=0)
                fecha= datetime.datetime.fromtimestamp(os.path.getmtime(os.path.join(path, file_[22:])))
                tabla_reemplazo = str.maketrans({"á":"a","é":"e","í":"i","ó":"o","ú":"u","ñ":"n","Á":"A","É":"E","Í":"I","Ó":"O","Ú":"U","Ñ":"N"})
                df.columns = df.columns.str.strip()
                df.columns = df.columns.str.replace(" ", "_", regex=True)
                df.columns = df.columns.str.translate(tabla_reemplazo)
                df.columns = df.columns.str.replace("[^0-9a-zA-Z_]", "", regex=True)
                df.columns = [insertar_raya_al_piso(nombre_columna) for nombre_columna in df.columns]
                df.columns = df.columns.str.upper()
                for fecha_mod in dic_fechas:
                    if fecha_mod in df.columns:
                        df[fecha_mod]=df[fecha_mod].apply(convertir_fecha)
                df= df.dropna(axis=1, thresh=2)
                df= df.dropna(axis=0, how='all')
                df['FILE_DATE'] = fecha
                df['FILE_NAME'] = file_[22:]
                df['FILE_YEAR'] = df['FILE_DATE'].dt.year
                df['FILE_MONTH']  = df['FILE_DATE'].dt.month
                print(df)
                if cargue_tabla == 1 and len(hojas_documento)>1:
                    df['HOJA_DATA'] = sheet
                    nombre_tabla1 = f"{nombre_tabla}"
                elif cargue_tabla == 0 and len(hojas_documento)>1:
                        nombre_tabla1= f"{nombre_tabla}_{sheet.lower()}"
                else:
                    nombre_tabla1 = f"{nombre_tabla}"
                for clave in dic_formatos:
                    if clave in df.columns:
                        df[clave] = df[clave].str.replace(",", ".", regex=True)
                        df[clave] = df[clave].str.replace("[^0-9-.]", "", regex=True)
                for clave in columnas_sin_espacio:
                    if clave in df.columns:
                        df[clave] = df[clave].str.replace("[^a-zA-Z0-9-@.,+]", "", regex=True)
                connection,cdn_connection,engine,bbdd_or = mysql_connection()
                logging.getLogger("user").info('Creacion Tabla temporal')
                tabla = Table(f"tb_{nombre_tabla1}", MetaData(), autoload_with = engine)
                nombre_columnas_nuevas = [c.name for c in tabla.c]
                diffCols = df.columns.difference(nombre_columnas_nuevas)
                listCols = list(diffCols)
                if len(listCols)!=0:
                    for i in range(len(listCols)):
                        logging.getLogger("user").info(f"Columna {i} agregada.")
                        connection.execute(text(f"ALTER TABLE `{bbdd_or}`.`{tabla.name}` ADD COLUMN `" + listCols[i] + "` VARCHAR(128)"))
                tabla = Table(f"tb_{nombre_tabla1}", MetaData(), autoload_with = engine)
                columnas_nuevas = [Column(c.name, c.type) for c in tabla.c]
                tmp = Table(f"{tabla.name}_tmp", MetaData(), *columnas_nuevas)
                tmp.drop(bind = engine,checkfirst=True)
                tmp.create(bind = engine)
                # Cargue del DataFrame de Dask en la base de datos MySQL.
                df.to_sql(tmp.name, cdn_connection, if_exists='append',index=False)
                connection.execute(text(f"INSERT IGNORE INTO {tabla.name} SELECT * FROM {tmp.name};"))
                tmp.drop(bind = engine,checkfirst=True)
                logging.getLogger("user").info('Insercion Tabla destino DW')
            except Exception as e:
                print(f"ERROR {e}")
                logging.getLogger("user").exception(e)
                raise
    except Exception as e:
        logging.getLogger("user").exception(e)
        raise

def toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla, asignacion, logs, columnas_sin_espacio,columnas_tabla):
    print("Entrando a esta función...")
    try:
        archivo_zip = path+"/"+file[22:]
        logging.getLogger("user").info(f"El archivo a cargar es {archivo_zip}")
        print("Se va a Descomprimir: ",archivo_zip)
        with zipfile.ZipFile(archivo_zip, 'r') as zip_ref:
            # Extrae todo el contenido en el directorio de destino
    
            zip_ref.extractall(r"/root/PROCESOS/BASESCLAROCOBRANZAPRUEBAS/src/ZIP/")

        file_to_load = Read_files_path(os.path.join("ZIP"),nombre_tabla,nombre_archivo)
        print("lista de archivos: ",file_to_load)
        for file_1 in file_to_load:
            nombre, extension = os.path.splitext(file_1)
            if extension in [".txt", ".csv"]:
                logging.getLogger("user").info(f"txt del zip")
                toSqlTxt(os.path.join(os.getcwd(),"ZIP"),nombre_tabla,file_1, dic_fechas,dic_formatos,separador,columnas_tabla)
            elif extension in [".xlsx"]:
                logging.getLogger("user").info(f"excel del zip")
                toSqlExcel(os.path.join("ZIP"),nombre_tabla,file_1, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, columnas_sin_espacio)
    except:
        raise
    finally:
        for file in os.listdir(os.path.join("ZIP")):
            os.remove(os.path.join("ZIP", file))         
# Función terciaria del archivo
def check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo, columnas_tabla, columnas_sin_espacio,cruze):
    logging.getLogger("user").info(f" archivo a cargar{file}")
    ini = time.time() # timepo de inicio del cargue 
    LOADED_FILES = f"{log_file_path1}//{nombre_tabla}.log" # ruta del log #//root//PROCESOS//BASESCLAROCOBRANZAPRUEBAS//src//LoadedFiles//

    with open(LOADED_FILES, "r") as f: # abrir el archivo del log
        logs     = f.read().splitlines()
        file_tmp = f.read().splitlines()
    
    logging.getLogger("user").info(f"La base '{file[22:]}' del {file[:20]} esta por cargarse") # log cargue
    logs.append(file) # Log archivo
    with open(LOADED_FILES, "w") as f:
            f.write(f"\n".join(logs))
    try:
        nombre, extension = os.path.splitext(file) # Obtener el nombre archivo y extención
        print(extension)
        # validacion de extenciones para saber que función usar 
        if extension in [".txt", ".csv"]:
            logging.getLogger("user").info(f"el archivo es un txt")
            toSqlTxt(path,nombre_tabla,file, dic_fechas,dic_formatos,separador, columnas_tabla,cruze)
        elif extension in [".xlsx", ".XLSX" ]:
            logging.getLogger("user").info(f"el archivo es un excel")
            toSqlExcel(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, columnas_tabla, columnas_sin_espacio)
        elif extension in [".zip", ".ZIP"]:
            logging.getLogger("user").info(f"el archivo es un zip")
            toSqlZip(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, nombre_archivo, cargue_tabla,asignacion, logs, columnas_sin_espacio,columnas_tabla)
        send(f"Se acaba de cargar la base {file[22:]} ")
        logging.getLogger("user").info(f"Base '{file[22:]}' del {file[:20]} recien cargada [ToSQL]")

    except Exception as e:
        with open(LOADED_FILES, "w") as f:
            f.write("\n".join(file_tmp))
        logging.getLogger("user").exception(f"Error en archivo: {file}\n{e}")
        send(f"Error cargando la base {file}: {e}")
    finally:
        fin = time.time()
        print(f"TOTAL TIEMPO EJECUCION {fin-inicio}")
        logging.getLogger("user").info(f"Tiempo total de ejecucion: {fin-ini} de {file}")
# Función primaria del archivo 
def scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion,columnas_tabla, columnas_sin_espacio,cruze):
    print(f"en Scan {nombre_tabla} -- {nombre_archivo}-- {path}")
    try:
        if os.path.exists(path): # validar si la ruta existe
            file_to_load = Read_files_path(path,nombre_tabla,nombre_archivo) # Obtener listado de los archivos a cargar 
            print(file_to_load)
            for file in file_to_load: # interar los archivos para el cargue
                print("cargando",file)
                # validacion y extandarización para el cargue de los archivos 
                check_and_add(path,nombre_tabla,file, dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, nombre_archivo, columnas_tabla, columnas_sin_espacio,cruze)   
        else:
            if hora == '08:00':
                # mesaje en caso de que no se encuentra la ruta deseada
                send(f"No ha habido cargue de la base {nombre_tabla} del {anio}-{mes}-{dia}") 
            logging.getLogger("user").info(f"no existe la ruta")
         
    except Exception as e:
        print(e)
