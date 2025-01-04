import streamlit as st
from search_by_reputation import logueos, lambda_handler, get_search_filters_dictionary
    
# Obtener el token de acceso y la cuenta de MercadoLibre
token_de_acceso, cuenta_meli, google_api_dict_list = logueos()

# Crear el título de la app
st.title("MercadoLibre Bot Scraper de vendedores con mejor reputacion y mayor cantidad de preguntas respondidas") # y cercania

# Tomar el input del usuario para el nombre del producto
producto = st.text_input("Nombre del item", "")

# Obtener los diccionarios con filtros
filters_values_dict, filters_values_id_dict, de_para_filtros_dict, de_para_filtros_values_dict = get_search_filters_dictionary(producto, token_de_acceso)

# Verificar si el nombre del producto ha sido ingresado
if producto != '':
        # Valores para los selectbox
        supported_cities = filters_values_dict["Ubicación"]
        supported_conditions = filters_values_dict["Condición"]
        supported_brands_names = filters_values_dict["Marca"]

        # Mostrar los selectbox con el argumento `key` para mantener los valores seleccionados en `session_state`
        city = st.selectbox("Ciudad", supported_cities, index=0, key="city")
        item_condition = st.selectbox("Condición", supported_conditions, index=0, key="item_condition")
        marca = st.selectbox("Marca", supported_brands_names, index=0, key="marca")

        # Verificar si los 3 selectbox tienen un valor seleccionado (diferente de 0)
        if city != 0 and item_condition != 0 and marca != 0:
        
                # Crear los filtros seleccionados
                city_id = de_para_filtros_dict[city]
                item_condition_id = de_para_filtros_dict[item_condition]
                marca_id = de_para_filtros_dict[marca]

                # ESTO TIENE EL FORMATO "state=TUxBUENBUGw3M2E1" & "BRAND=130159" => "nombre filtro = valor_id"
                filtro_ubicacion = de_para_filtros_values_dict[city_id] + '=' + de_para_filtros_dict[city]
                filtro_condicion = de_para_filtros_values_dict[item_condition_id] + '=' + de_para_filtros_dict[item_condition]
                filtro_marca = de_para_filtros_values_dict[marca_id] + '=' + de_para_filtros_dict[marca]

                # Crear la cadena de filtros
                filtros = '&' + filtro_ubicacion + '&' + filtro_condicion + '&' + filtro_marca

                # Botón de submit para procesar los filtros
                submit = st.button("Submit")

                # Si el botón es clicado, ejecutar la función
                if submit:
                        df_resultados = lambda_handler(producto, token_de_acceso, filtros, google_api_dict_list)
                        df_resultados['item_url'] = df_resultados['item_url'].apply(lambda x: f'<a href="{x}" target="_blank">{x}</a>')

                        # Display the length of the results list.
                        st.write(f"Number of results: {len(df_resultados)}")

                        # st.dataframe(df_resultados)
                        st.markdown(df_resultados.to_html(escape=False, index=False), unsafe_allow_html=True)