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


#Creamos la conexion
#instance es el nombre de la BD
#UID el usuario
#PWD la contraseña
#Driver es el Driver necesario
Connection_String = 'Driver={Oracle in XE};DBQ=localhost:1521/xe;Uid=system;Pwd=12345;'

conn = pypyodbc.connect(Connection_String)

# Se puede establecer el autocommit de la siguiente manera:
# conn.autocommit=True

#Creamos un cursor para la conexion
cursor = conn.cursor()
cursor.execute("DELETE FROM POBLACION WHERE dni = 999999999")

#Ejecutamos una consulta sobre la base de datos
cursor.execute('SELECT * FROM POBLACION')

#Extraemos la primera tupla
row = cursor.fetchone()

#Mostramos el DNI de la primera tupla
print('dni', row['dni'])

#Se pueden extraer todas de una vez
rows = cursor.fetchall()

#Se muestran todas las tuplas
for persona in rows:
    print(persona['dni'], ',', persona['nombre'])

print("""\n\nEjecucion con parámetros\n\n""")
#También acepta parámetros en las consultas
sql='select * from poblacion WHERE dni=?'

cursor.execute(sql, [333445555]) #Debe ir entre corchetes
row=cursor.fetchone()

print(row['dni'], ',', row['nombre'])

#Insercion y actualizacion
cursor.execute("INSERT INTO poblacion VALUES (999999999,'Cristian','Dominguez','Gomez','08/05/1993','Calle Larga','06009','H',27000,1000,1000,1000,1)")
cursor.commit()#Necesario para guardar los cambios

#Mostramos la tupla recien insertada
cursor.execute(sql, [999999999]) #Debe ir entre corchetes
row=cursor.fetchone()

print("""\n\nTupla insertada\n\n""")

print(row)

cursor.execute("UPDATE poblacion SET ingresos=15000 WHERE dni=999999999")
cursor.commit()#Necesario para guardar los cambios

#Mostramos la tupla recien insertada
cursor.execute(sql, [999999999]) #Debe ir entre corchetes
row=cursor.fetchone()
print("""\n\nTupla actualizada\n\n""")

print(row)

#Borrado
cursor.execute("DELETE FROM POBLACION WHERE dni = 999999999")
cursor.commit()
#Cerramos conexion y cursor
cursor.close()
conn.close()