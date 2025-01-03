import streamlit as st
from search_by_reputation import logueos, lambda_handler, get_search_filters_dictionary
import boto3, boto3.session
from botocore.exceptions import ClientError

def get_secret_value_aws(secret_name):
    # Leer las credenciales desde st.secrets
    AWS_ACCESS_KEY_ID = st.secrets["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = st.secrets["AWS_SECRET_ACCESS_KEY"]
    region_name = st.secrets.get("AWS_REGION", "us-east-2")

    # Crear sesión con las credenciales
    session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=region_name
    )
    client = session.client(service_name="secretsmanager")

    try:
        # Obtener el secreto
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return secret
    except ClientError as e:
        st.error(f"Error al obtener el secreto: {e}")
        raise e
    
# Obtener el token de acceso y la cuenta de MercadoLibre
token_de_acceso, cuenta_meli = logueos()

# Crear el título de la app
st.title("MercadoLibre Bot Scraper de vendedores con mejor reputacion, cercania y mayor cantidad de preguntas respondidas")

# Tomar el input del usuario para el nombre del producto
producto = st.text_input("Nombre del item", "")

# Verificar si el nombre del producto ha sido ingresado
# if producto:

# Obtener los diccionarios con filtros
filters_values_dict, filters_values_id_dict, de_para_filtros_dict, de_para_filtros_values_dict = get_search_filters_dictionary(producto, token_de_acceso)

# Valores para los selectbox
supported_cities = filters_values_dict["Ubicación"]
supported_conditions = filters_values_dict["Condición"]
supported_brands_names = filters_values_dict["Marca"]

# Mostrar los selectbox con el argumento `key` para mantener los valores seleccionados en `session_state`
city = st.selectbox("Ciudad", supported_cities, index=0, key="city")
item_condition = st.selectbox("Condición", supported_conditions, index=0, key="item_condition")
marca = st.selectbox("Marca", supported_brands_names, index=0, key="marca")

# # Verificar si los 3 selectbox tienen un valor seleccionado (diferente de 0)
# if city != 0 and item_condition != 0 and marca != 0:

# Crear los filtros seleccionados
filtro_ubicacion = de_para_filtros_values_dict[city] + '=' + de_para_filtros_dict[city]
filtro_condicion = de_para_filtros_values_dict[item_condition] + '=' + de_para_filtros_dict[item_condition]
filtro_marca = de_para_filtros_values_dict[marca] + '=' + de_para_filtros_dict[marca]

# Crear la cadena de filtros
filtros = '&' + filtro_ubicacion + '&' + filtro_condicion + '&' + filtro_marca

# print(filtros)

# exit()

# Botón de submit para procesar los filtros
submit = st.button("Submit")

# Si el botón es clicado, ejecutar la función
if submit:
        df_resultados = lambda_handler(producto, token_de_acceso, filtros)

        # Mostrar los resultados
        st.write('Resultados dataframe:')
        st.dataframe(df_resultados)
        
        # Display the length of the results list.
        st.write(f"Number of results: {len(df_resultados)}")