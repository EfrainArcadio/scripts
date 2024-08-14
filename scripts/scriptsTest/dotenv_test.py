from dotenv import load_dotenv
import os

# Carga las variables de entorno desde el archivo .env
load_dotenv()

# Accede a las variables como si fueran variables de entorno normales
HOST = os.getenv('HOST')
DATABASE = os.getenv('DATABASE')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
PORT =os.getenv('PORT')
print(HOST)
print(DATABASE)
print(USER)
print(PASSWORD)
print(PORT)
