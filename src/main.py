# Importar librerias 
from Imports import *
# Importar ruta de controllers
sys.path.append(os.path.join("Controllers"))
from controller import *
from Functions import *

# Obterner la colección de rutas a consultar   
with open(os.path.join("config","config_files.json")) as archivo_config:
    configuracion = json.load(archivo_config)
archivos = configuracion["ejecuciones"].keys()
coleccion = configuracion["ejecuciones"]

# Iterar las rutas para poceder hacer el proceso de cada una de las rutas 
for i in range(len(list(archivos))):
    key1=list(archivos)[i].strip("'")
    
    # variables globales
    nombre_archivo = coleccion[key1]["varibles"][0] 
    nombre_tabla   = coleccion[key1]["varibles"][1].strip("'")
    path_0         = coleccion[key1]["varibles"][2].strip("'")
    separador = coleccion[key1]["opcion_path"][1].strip("'")
    cargue_tabla = coleccion[key1]["opcion_cargue_tabla"] 
    asignacion = coleccion[key1]["asignacion"]
    # Selección de tipo de ruta a leer 
    if  coleccion[key1]["opcion_path"][0].strip("'") == "1":
        path = f"{path_0}//2023//11//{dia}"
    elif coleccion[key1]["opcion_path"][0].strip("'") == "2":
        path = f"{path_0}//2023//11"
    elif coleccion[key1]["opcion_path"][0].strip("'") == "3":
        path = f"{path_0}/2023"
    elif coleccion[key1]["opcion_path"][0].strip("'") == "4":
        path = f"{path_0}"
        
    # Extracion de dicionarios a leer
    dic_fechas = coleccion[key1]["fechas"]
    dic_formatos = coleccion[key1]["formatos"]
    dic_hojas = coleccion[key1]["sheets"]
    columnas_tabla = coleccion[key1]["columnas"]
    columnas_sin_espacio =coleccion[key1]["columnas_sin_espacio"]
    if __name__ == '__main__':
        # Ejecucion de función primaria 
        scan_folder(path,nombre_tabla,nombre_archivo,dic_fechas,dic_formatos,dic_hojas,separador, cargue_tabla, asignacion, columnas_tabla, columnas_sin_espacio)

