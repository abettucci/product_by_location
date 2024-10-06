# Utiliza una imagen base oficial de Python
FROM public.ecr.aws/lambda/python:3.9

# Establece el directorio de trabajo en el contenedor
WORKDIR /app

# Copia el archivo de requisitos en el directorio de trabajo
COPY requirements.txt ./

# Instala las dependencias
RUN python3.9 -m pip install -r requirements.txt -t .

# Copia el resto del código de la aplicación en el contenedor
COPY . .

# Establece el comando por defecto para ejecutar tu aplicación
CMD ["python", "main.py"]