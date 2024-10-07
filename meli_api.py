import requests
import json
import boto3
import gspread
import geocoder
import os
import pandas as pd
import numpy as np
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.errors import HttpError
from gspread.exceptions import APIError
from botocore.exceptions import ClientError
from rapidfuzz import fuzz
import time
from datetime import datetime, timedelta
import pytz

def get_secret_value_aws(secret_name):
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name="us-east-2")
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    secret = get_secret_value_response['SecretString']
    return secret

def get_request(url, headers):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response

def post_request(url, data, headers):
    response = requests.post(url, data=data, headers=headers)
    response.raise_for_status()
    return response

def put_request(url, data, headers):
    response = requests.put(url, data=data, headers=headers)
    response.raise_for_status()
    return response

def delete_request(url, headers):
    response = requests.delete(url, headers=headers)
    response.raise_for_status()
    return response

# Funcion para obtener el id de un usuario de MELI con su token de acceso
def get_user_id(token_de_acceso, meli_api_calls):
    url = "https://api.mercadolibre.com/users/me"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = get_request(url, headers=headers)
    meli_api_calls += 1
    return response.json(), meli_api_calls

# Funcion para regenerar el token de acceso a la API de MELI 
def get_access_token(app_id, secret_client_id, refresh_token, meli_api_calls):
    url = "https://api.mercadolibre.com/oauth/token"
    payload = f"grant_type=refresh_token&client_id={app_id}&client_secret={secret_client_id}&refresh_token={refresh_token}"
    headers = {'accept': 'application/json','content-type': 'application/x-www-form-urlencoded'}
    response = requests.request("POST", url, headers=headers, data=payload)
    data = json.loads(response.text)
    proximo_refresh_token = data.get('refresh_token', None)
    access_token = data.get('access_token', None)
    t_expiracion = data.get('expires_in', None)
    meli_api_calls += 1
    return proximo_refresh_token, access_token, t_expiracion, meli_api_calls

def get_mercado_libre_token(current_time, worksheet_tokens_mio, fila, meli_api_calls, google_write_api_calls):
    # Obtenemos los tokens del sheets
    refresh_token = worksheet_tokens_mio.cell(fila, 1).value  
    access_token = worksheet_tokens_mio.cell(fila, 2).value  
    t_expiracion = worksheet_tokens_mio.cell(fila, 3).value 
    app_id = worksheet_tokens_mio.cell(fila, 6).value 
    secret_client_id = worksheet_tokens_mio.cell(fila, 7).value 
    cuenta_meli, meli_api_calls = get_user_id(access_token, meli_api_calls)
    cuenta_meli = cuenta_meli.get('nickname',None)
    print('Cuenta: ', cuenta_meli)

    t_expiracion = datetime.strptime(t_expiracion, '%m/%d/%Y %H:%M:%S')

    # Si todavia no se expiraron los tokens para pinguear la API de MercadoLibre, seguimos utilizando el ultimo token
    if current_time < t_expiracion:
        token_de_acceso = access_token
    else:
        # Generamos un nuevo refresh_token y acces_token con el get_Access_token() para renovar los tokens de la API de MELI
        proximo_refresh_token, token_de_acceso,tiempo_de_expiracion, meli_api_calls = get_access_token(app_id, secret_client_id, refresh_token, meli_api_calls)

        # Hacemos un overwrite en el Sheets para reemplazar el refresh_token, access_token y fecha de expiracion viejos por los nuevos
        horario_expiracion_refresh_token = current_time + timedelta(seconds=tiempo_de_expiracion) #'expires in' = 21600 seg = 6 hs 
        horario_expiracion_refresh_token = horario_expiracion_refresh_token.strftime('%m/%d/%Y %H:%M:%S')
        horario_expiracion_refresh_token = horario_expiracion_refresh_token.replace(' ', ' 0', 1) if horario_expiracion_refresh_token[11] == ' ' else horario_expiracion_refresh_token

        worksheet_tokens_mio.update_cell(fila, 1, proximo_refresh_token)
        worksheet_tokens_mio.update_cell(fila, 2, token_de_acceso)
        worksheet_tokens_mio.update_cell(fila, 3, str(horario_expiracion_refresh_token))
        google_write_api_calls += 3

    return access_token, cuenta_meli

# Funcion que realza una accion sobre una hoja de google sheets en funcion del parametro "funcion" que define
# si solo debe abrir un sheets, abrir una hoja en particular de un sheets o abrir y obtener todos los datos de una hoja
def make_read_api_call(funcion, parametros, hoja, slice1, slice2, google_api_dict_list):
    try:
        if funcion == 'open_by_key':
            resultado = hoja.open_by_key(parametros)
        if funcion == 'get_worksheet_by_id':
            resultado = hoja.get_worksheet_by_id(parametros)
        elif funcion == 'get_all_values':
            resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
        else: #get_worksheet_by_id_and_get_all_values
            if slice2 == '':
                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:]
            else:
                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1]
    except HttpError as e:
        if e.resp.status == 429:
            try:
                creds = ServiceAccountCredentials.from_json_keyfile_dict(
                    google_api_dict_list[1], 
                    ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
                gc = gspread.authorize(creds) 
                # sh = gc.open_by_key('1AbwRrOEFrcCXR2v8_djqckhLjJOM_2r8S1VwBwhS5Q0')

                if funcion == 'open_by_key':
                    resultado = gc.open_by_key(parametros)
                elif funcion == 'get_worksheet_by_id':
                    resultado = hoja.get_worksheet_by_id(parametros)
                elif funcion == 'get_all_values':
                    resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
                else: #get_worksheet_by_id_and_get_all_values
                    if slice2 == '':
                        resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:]
                    else:
                        resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1]
            except:
                if e.resp.status == 429:
                    # Handle rate limit exceeded error with exponential backoff
                    wait_time = 1  # Initial wait time in seconds
                    max_retries = 5  # Maximum number of retries
                    retries = 0

                    while retries < max_retries:
                        print(f"Rate limit exceeded. Waiting for {wait_time} seconds...")
                        time.sleep(wait_time)
                        try:
                            if funcion == 'open_by_key':
                                resultado = gc.open_by_key(parametros)
                            elif funcion == 'get_worksheet_by_id':
                                resultado = hoja.get_worksheet_by_id(parametros)
                            elif funcion == 'get_all_values':
                                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()
                            else: #get_worksheet_by_id_and_get_all_values
                                resultado = hoja.get_worksheet_by_id(parametros).get_all_values()[slice1:slice2]
                            break
                        except HttpError as e:
                            if e.resp.status == 429:
                                # Increase wait time exponentially for the next retry
                                wait_time *= 2
                                retries += 1
                            else:
                                # Handle other HTTP errors
                                raise
                else:
                    # Handle other HTTP errors
                    raise
    return resultado

def google_sheets_auth(google_read_api_calls):
    # Obtenemos las credenciales de la API de MercadoLibre con un secreto del SecretManager de AWS pasandole la ruta del secreto
    google_api_dict_list = []
    google_key_locations = [
        'abettucci/MELIproject/Google_API_JSON_Key_File3',
        'abettucci/MELIproject/Google_API_JSON_Key_File']
    for api_dict in google_key_locations:
        secret = get_secret_value_aws(api_dict)
        secret_data = json.loads(secret)
        key_dict = {
            "private_key_id" : secret_data.get('private_key_id'),
            "type" : secret_data.get('type'),
            "project_id" : secret_data.get('project_id'),
            "client_id" : secret_data.get('client_id'),
            "client_email" : secret_data.get('client_email'),
            "private_key" : secret_data.get('private_key')}
        google_api_dict_list.append(key_dict)

    # Leemos el sheets de Tokens que contiene los ultimos tokens para renovarlos o volver a utilizarlos e iniciamos un cliente de Google
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        google_api_dict_list[0], 
        ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive'])
    gc = gspread.authorize(creds) 
    api_key = google_api_dict_list[0]["private_key_id"]
    sh = gc.open_by_key('1AbwRrOEFrcCXR2v8_djqckhLjJOM_2r8S1VwBwhS5Q0')
    worksheet_tokens_mio = make_read_api_call('get_worksheet_by_id',1387040377,sh,'','', google_api_dict_list)
    google_read_api_calls += 1

    return worksheet_tokens_mio, gc, google_read_api_calls, google_api_dict_list

def get_item_seller_id(item_id, token_de_acceso):
    url = f"https://api.mercadolibre.com/items/{item_id}?include_attributes=all"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()["seller_id"]

def get_item_description(item_id, token_de_acceso):
    url = f"https://api.mercadolibre.com/items/{item_id}/description"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()["plain_text"]

def get_answered_questions(item_id, token_de_acceso):
    url = f"https://api.mercadolibre.com/questions/search?item={item_id}&api_version=4"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload)

    faq_dict = dict()
    preguntas = response.json()["questions"]

    for pregunta in preguntas:
        question = pregunta["text"]
        answer = pregunta["answer"]["text"].lower()
        faq_dict[question] = answer
    
    return faq_dict
    
def get_seller_nickname_and_city(seller_id, token_de_acceso):
    url = f"https://api.mercadolibre.com/users/{seller_id}"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()["address"]["city"], response.json()["nickname"]

def find_company_name_in_answers(item_id, token_de_acceso):
    faq_dict = get_answered_questions(item_id, token_de_acceso)
    hints = ["somos", "estamos en", "tienda", "saludos"]
    for answer in faq_dict.values():
        for hint in hints:
            if hint in answer:
                print(f"La palabra {hint} " + "esta dentro de la respuesta")

# Por ahi es mejor dar un listado de opciones y que solo ingreses un numero y te busque con el id exacto de la categoria en vez de tipearla
def get_codigo_categorias_por_nombre_de_categoria(nombre_categoria, token_de_acceso):
    url = "https://api.mercadolibre.com/sites/MLA/categories"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload).json()[0]
    categorias = {item['name']: item['id'] for item in response}

    # Iteramos en cada nombre de categoria que es la llave del diccionario. Chequeamos que el grado de coincidencia  >= threshold
    threshold = 80
    for categoria in list(categorias.keys()):
        similarity = fuzz.token_set_ratio(nombre_categoria.lower(), categoria.lower())
        if similarity >= threshold:
            id_categoria = categorias[categoria]
            break
    
    return id_categoria

# Busqueda por nombre de producto
def get_items_from_category_brand_search(search_type, category_name, token_de_acceso):
    
    my_product_category_id = get_codigo_categorias_por_nombre_de_categoria(category_name)

    # Esta ordenado por precio ascendente
    if search_type == 'scan all':
        url = f"https://api.mercadolibre.com/sites/MLA/search?category={my_product_category_id}&catalog_product_id=null" #&BRAND={my_product_brand_id}
        payload = {}
        headers = {'Authorization': 'Bearer ' + token_de_acceso}
        response = get_request(url, headers=headers)
        items = response.json().get('results', [])

    elif search_type == 'scan top seller':
        url = f"https://api.mercadolibre.com/sites/MLA/search?category={my_product_category_id}&power_seller=yes&catalog_product_id=null" #&BRAND={my_product_brand_id}
        payload = {}
        headers = {'Authorization': 'Bearer ' + token_de_acceso}
        response = get_request(url, headers=headers)
        items = response.json().get('results', [])    

    return items

def get_items(next_page_url, token_de_acceso):
    # Traer en orden de relevancia o de winner de catalogo (hay un parametro que es "order_backend" en catalogo creo)
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", next_page_url, headers=headers, data=payload)
    data = response.json()
    return data

# Busqueda por nombre de producto. Docu: https://developers.mercadolibre.com.ar/es_ar/items-y-busquedas#Obtener-%C3%ADtems-de-una-consulta-de-b%C3%BAsqueda
def get_items_from_name_search(item_name, token_de_acceso):
    # El limit es maximo de a 100 publicaciones por pagina. Con el search_type = scan podemos traer hasta 1000 registros (creo que no funca para busqueda por query search)
    url = f"https://api.mercadolibre.com/sites/MLA/search?q={item_name}"
    payload = {}
    headers = {'Authorization': 'Bearer ' + token_de_acceso}
    response = requests.request("GET", url, headers=headers, data=payload)
    data = response.json()

    if data is not None:
        total_results = data.get('paging', {}).get('total', 0)
        items_per_page = data.get('paging', {}).get('limit', 0)
        total_pages = (total_results + items_per_page - 1) // items_per_page
        all_items = data.get('results', [])
        for page in range(1, total_pages):
            offset = page * items_per_page
            next_page_url = f"{url}&offset={offset}"
            page_data = get_items(next_page_url, token_de_acceso)
            if page_data is not None:
                all_items.extend(page_data.get('results', []))
            else:
                print(f"Failed to fetch data for page {page}")
    else:
        print("Failed to fetch data for the initial page")

    return all_items

def find_company_name_in_description(item_id, token_de_acceso):

    # Aca deberia meter un GET de todos los items que se venden pero filtrando por CIUDAD y PROVINCIA para acortar los resultados
    item_description =  get_item_description(item_id, token_de_acceso).lower()
    # print(item_description)

    hints = ["somos", "estamos en", "tienda", "saludos"]
    for hint in hints:
        if hint in item_description:
            # print(f"La palabra {hint} " + "esta dentro de la descripcion del item")

            seller_id = get_item_seller_id(item_id, token_de_acceso)
            seller_city, seller_nickname = get_seller_nickname_and_city(seller_id, token_de_acceso)

            return seller_city, seller_nickname

def find_company_location():
    # Aca podria buscar en google el nombre de la empresa y con eso buscar con la API de google maps donde se encuentra o scrapear
    # si tiene pagina web si dice las sucursales y su ubicacion.

    return None

def ver_data_schema(result):
    for field in result.keys():
        print(field) 
        if isinstance(result.get(field), dict):
            dict1 = result.get(field)
            for subfield in dict1.keys():
                if isinstance(dict1.get(subfield), dict):
                    dict2 = dict1.get(subfield)
                    for subfield2 in dict2.keys():
                        print(subfield2, type(dict2.get(subfield2)))
                else: #es lista o string o int o float o bool
                    print(subfield, type(dict1.get(subfield)))
        else:
            print(field, type(result.get(field)))

def get_coordinates_with_address(address, bing_map_api_key):
    g = geocoder.bing(address, key = bing_map_api_key)
    result = g.json    
    # Check if the geocoding was successful
    # data = {
    #     'Latitude': result.get('lat', None),
    #     'Longitude': result.get('lng', None),
    #     'City': result.get('city', None),
    #     'Country': result.get('country', None),
    #     'Neighborhood': result.get('neighborhood', None),
    #     'Postal': result.get('postal', None),
    #     'Locality': result.get('locality', None),
    #     'AdminDistrict': result.get('raw',[]).get('address',[]).get('adminDistrict', None),
    #     'AdminDistrict2': result.get('raw',[]).get('address',[]).get('adminDistrict2', None),
    #     'PostalCode': result.get('raw',[]).get('address',[]).get('postalCode', None),
    #     'State': result.get('state', None),
    #     'Street': result.get('street', None)
    # }
        
    return result.get('lat', None), result.get('lng', None)
 
def obtener_georeferencia(nombre_del_local, ciudad, bing_map_api_key):
    # Construir la consulta con nombre del lugar, ciudad y país
    query = f"{nombre_del_local}, {ciudad}, {"Argentina"}"

    # URL de la Bing Maps API para realizar la búsqueda
    url = f"http://dev.virtualearth.net/REST/v1/Locations?query={query}&key={bing_map_api_key}"

    # Realizar la solicitud
    response = requests.get(url)
    resultados = response.json()

    # Verifica si se encontraron resultados
    if resultados['resourceSets'][0]['estimatedTotal'] > 0:
        lugar = resultados['resourceSets'][0]['resources'][0]
        nombre = lugar['name']
        direccion = lugar['address']['formattedAddress']
        coordenadas = lugar['point']['coordinates']
        latitud = coordenadas[0]
        longitud = coordenadas[1]
    else:
        print("No se encontró el lugar.")

    return latitud, longitud

def obtener_direccion_por_nombre_comercio_gmaps():


    return None

def obtener_nombres_comercios_meli():



    return None

# Función para calcular la distancia usando la fórmula de Haversine
def obtener_distancia_entre_local_y_ubicacion_actual(lat1, lon1, lat2, lon2):
    # Convertir de grados a radianes
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])

    # Diferencias de latitud y longitud
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    # Aplicar fórmula de Haversine
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    
    # Radio de la Tierra en kilómetros
    r = 6371
    return round(c * r,1)

def lambda_handler_location(item_name, token_de_acceso):

    items_scrapeados =  get_items_from_name_search(item_name, token_de_acceso) #Ojo con el tema de la paginacion!

    dict_vendors_name_and_city = dict()
    data_diccionario_items = []

    for item in items_scrapeados:
        item_id_con_mla = item['id']
        item_url = item['permalink']
        try:
            ciudad, nombre_del_local = find_company_name_in_description(item_id_con_mla, token_de_acceso)

            # Rellenamos el diccionario de vendedores y ubicacion
            if nombre_del_local not in list(dict_vendors_name_and_city.keys()):
                dict_vendors_name_and_city[nombre_del_local] = ciudad

            # Rellenamos el diccionario de items, url y vendedor (luego joinearlo con un merge de dos df)
            for nombre_del_local, ciudad in dict_vendors_name_and_city.items():
                fila = {
                    "nombre_del_local" : nombre_del_local,
                    "item_id" : item_id_con_mla,
                    "item_url" : item_url
                } 
                data_diccionario_items.append(fila)

        except:
            pass

    # Obtener la API key desde las variables de entorno
    bing_map_api_key = os.getenv('API_KEY')

    if not bing_map_api_key:
        raise ValueError("No se encontró la API key")

    # Coordenadas de tu ubicación actual (Aguero 1595)
    mi_latitud, mi_longitud = get_coordinates_with_address('Agüero 1595, Capital Federal, Argentina', bing_map_api_key)

    data = []
    for nombre_del_local, ciudad in dict_vendors_name_and_city.items():
        latitud, longitud = obtener_georeferencia(nombre_del_local, ciudad, bing_map_api_key)
        fila = {
            "nombre_del_local" : nombre_del_local,
            "ciudad" : ciudad,
            "latitud" : latitud,
            "longitud": longitud,
            "distancia": obtener_distancia_entre_local_y_ubicacion_actual(mi_latitud, mi_longitud, latitud, longitud),
        } 
        data.append(fila)

    df_distancias_vendors = pd.DataFrame(data)
    df_items_vendors = pd.DataFrame(data_diccionario_items)

    df_final = df_items_vendors.merge(df_distancias_vendors, on='nombre_del_local', how='left')

    print(df_final.sort_values(by='distancia', ascending=True))

    # df_georeferencias['distancia_km'] = df_georeferencias.apply(lambda row: obtener_distancia_entre_local_y_ubicacion_actual(mi_latitud, mi_longitud, row['latitude'], row['longitude']), axis=1)

    return df_final