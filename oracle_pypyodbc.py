import pypyodbc

#---------------------------------------------
#-----Conexion a una base de datos Oracle-----
#--------utilizando la librería pyodbc--------
#---------------------------------------------

#Si no encontramos el driver, descomentar
#las siguientes lineas para listar todos
#y seleccionar el adecuado

#drivers_list = sorted(pypyodbc.drivers())
#for driver_name in drivers_list:
#    print(driver_name)

###############################################
############DEFINICION DE MÉTODOS##############
###############################################

"""
    Método que se encarga de listar todos los drivers
    disponibles en la máquina cuyo objetivo es
    establecer conexión con una base de datos
"""
def listarDrivers():
    drivers_list = pypyodbc.drivers()
    for driver in drivers_list:
        print(driver)

"""
    Método para crear la conexión con una base de datos
    Salida: devuelve la conexion creada
"""
def crearConexion():
    """
        Oracle in XE: es el driver para conectarse con la BD de Oracle
        DBQ: indica servidor:puerto/BD
        Uid: indica el usuario para la conexión
        Pwd: indica la contraseña para el usuario anterior
    """
    connection_string='Driver={Oracle in XE};DBQ=localhost:1521/xe;Uid=system;Pwd=12345;'
    conexion = pypyodbc.connect(connection_string)
    #Con la siguiente línea se indica que los commits se realizan automáticamente
    #conexion.autocommit=True

    return conexion #Devolvemos la conexion


"""
    Método que borra una tupla de la tabla poblacion
    Parámetros:
        conexion: es la variable donde se almacena la conexión con
        la base de datos.
        dni: es la variable que almacena el dni de la persona
        que se va a borrar de la base de datos.
"""
def borrarPersonaPoblacion(conexion, dni):
    cursor = conexion.cursor() #Se obtiene un cursor de la conexion
    cursor.execute("DELETE FROM POBLACION WHERE dni=?",[dni]) # Se ejecuta la consulta
    cursor.close() #Se cierra el cursor

"""
    Método para mostrar todas las tuplas de la tabla poblacion
    Parámetros:
        conexion: almacena la conexión con la base de datos
"""
def mostrarPoblacion(conexion):
    print("-------MOSTRAR TODA LA TABLA POBLACIÓN-----------")

    cursor = conexion.cursor() #Se obtieneu n cursor de la conexion
    cursor.execute("SELECT * FROM POBLACION") #Se ejecuta la consulta
    #Se extraen todas las tuplas
    rows = cursor.fetchall()
    #Se recorre el resultado
    for row in rows:
        print("DNI: ",row['dni'])
        print("NOMBRE:",row['nombre'])
        print("APELLIDOS:", row['apellido1']," ",row['apellido2'])
        print("DIRECCION:", row['direccion'])
        print("CÓDIGO POSTAL:", row['cp'])
        print("FECHA DE NACIMIENTO:", row['fechanac'])
        print("SEXO:", row['sexo'])
        print("INGRESOS:",row['ingresos'])
        print("GASTOS FIJOS:",row['gastosfijos'])
        print("GASTOS ALIMENTACIÓN:",row['gastosalim'])
        print("GASTOS ROPA:",row['gastosropa'])
        print("SECTOR:",row['sector'])
        print("----------------------------------------------------------------")
    cursor.close()

def buscarPersona(conexion,dni):
    #Se obtiene el cursor
    cursor = conexion.cursor()

    #Se ejecuta la consulta
    cursor.execute("SELECT * FROM POBLACION WHERE dni=?",[dni])

    #Se extrae la tupla
    persona = cursor.fetchone()

    return persona

"""
    Método para insertar una persona en la tabla poblacion
    Parámetros:
        conexion: variable que almacena la conexión con la base de datos.
        dni: dni de la persona a insertar.
        nombre: nombre de la persona a insertar.
        apellido1: primer apellido de la persona a insertar.
        apellido2: segundo apellido de la persona a insertar.
        direccion: dirección de la persona a insertar.
        cp: código postal de la persona a insertar.
        sexo: sexo de la persona a insertar.       
        ingresos: ingresos de la persona a insertar.
        gastosfijos: gastos fijos de la persona a insertar.
        gastosalim: gastos en alimentación de la persona a insertar.
        gastosropa: gastos en ropa de la persona a insertar.
        sector: sector al que pertenece la persona a insertar.
"""
def insertarPoblacion(conexion,dni,nombre,apellido1,apellido2,
                      direccion,fechanac,cp,sexo,ingresos,gastosfijos,gastosalim,
                      gastosropa,sector):

    print("-----INSERCION DE UNA PERSONA-----")

    #Se obtiene el cursor
    cursor = conexion.cursor()

    #Construcción de la consulta sql para ejecutarla con parámetros
    sql = 'INSERT INTO POBLACION VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'

    #Ejecución de la consulta con los parámetros
    cursor.execute(sql,[dni,nombre,apellido1,apellido2,fechanac,direccion,
                          cp,sexo,ingresos,gastosfijos,gastosalim,gastosropa,
                          sector])
    # Se ejecuta el commit para que se haga efectivo el cambio
    cursor.commit()

    #Se cierra el cursor
    cursor.close()

"""
    Método para insertar una persona en la tabla poblacion
    Parámetros:
        conexion: variable que almacena la conexión con la base de datos.
        dni: dni de la persona a actualizar.
        nombre: nombre de la persona a actualizar.
        apellido1: primer apellido de la persona a actualizar.
        apellido2: segundo apellido de la persona a actualizar.
        direccion: dirección de la persona a actualizar.
        cp: código postal de la persona a actualizar.
        sexo: sexo de la persona a actualizar.       
        ingresos: ingresos de la persona a actualizar.
        gastosfijos: gastos fijos de la persona a actualizar.
        gastosalim: gastos en alimentación de la persona a actualizar.
        gastosropa: gastos en ropa de la persona a actualizar.
        sector: sector al que pertenece la persona a actualizar.
"""
def actualizarUsuario(conexion,dni,nombre,apellido1,apellido2,
                      direccion,fechanac,cp,sexo,ingresos,gastosfijos,gastosalim,
                      gastosropa,sector):
    #Se obtiene el cursor
    cursor = conexion.cursor()


    #Construcción de la consulta sql para ejecutarla con parámetros
    sql = """UPDATE POBLACION SET nombre=?,apellido1=?,apellido2=?,fechanac=?,direccion=?,
             cp=?,sexo=?,ingresos=?,gastosfijos=?,gastosalim=?,gastosropa=?,sector=? WHERE dni=?"""

    #Ejecución de la consulta con los parámetros
    cursor.execute(sql,[nombre,apellido1,apellido2,fechanac,direccion,
                          cp,sexo,ingresos,gastosfijos,gastosalim,gastosropa,
                          sector,dni])

    # Se ejecuta el commit para que se haga efectivo el cambio
    cursor.commit()

    #Se cierra el cursor
    cursor.close()

"""
    Método para borrar un usuario de la base de datos
    Parámetros:
        conexion: almacena la conexión de la base de datos.
        dni: dni de la persona a borrar.
"""
def borrarUsuario(conexion,dni):
    print("-----ACTUALIZACIÓN DE UNA PERSONA-----")
    # Se obtiene el cursor
    cursor = conexion.cursor()
    #Se ejecuta la consulta
    cursor.execute("DELETE FROM POBLACION WHERE dni='?'",[dni])

    #Se ejecuta el commit para que se haga efectivo el cambio
    cursor.commit()

    #Se cierra el cursor
    cursor.close()


###LLAMADAS A PROCEDIMIENTOS###
conexion=crearConexion()

mostrarPoblacion(conexion)

#Se borra la persona del ejemplo en caso de que existiera
borrarPersonaPoblacion(conexion,'999999999')

insertarPoblacion(conexion, '999999999','Cristian','Dominguez','Gomez','Calle Caupolicán',
                  '08/05/1993','10005','H','10000','2000','3000','3000','1')


#Se busca la persona insertada
print(buscarPersona(conexion,'999999999'))

#Se actualizan los ingresos a 25K
actualizarUsuario(conexion,'999999999','Cristian','Dominguez','Gomez','Calle Caupolicán',
                  '08/05/1993','10005','H','25000','2000','3000','3000','1')

#Se muestra a la persona cuyos ingresos han sido actualizados
print(buscarPersona(conexion,'999999999'))

#Se borra la persona
borrarPersonaPoblacion(conexion,'999999999')

#Se cierra el cursor
conexion.close()

#Se muestran los drivers
listarDrivers()