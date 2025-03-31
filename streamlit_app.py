import streamlit as st
import requests
import time
import base64
import pandas as pd
import re

class DataForSEOAPI:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {auth}',
            'Content-Type': 'application/json'
        }
    
    def post_task(self, query, location="Paris,Ile-de-France,France", language="fr"):
        endpoint = "https://api.dataforseo.com/v3/serp/google/organic/task_post"
        
        # Structure correcte de la requête
        payload = [
            {
                "keyword": query,
                "location_name": location,
                "language_name": language,
                "device": "desktop",
                "os": "windows",
                "depth": 200  # Correspond au nombre de résultats demandés
            }
        ]
        
        try:
            response = requests.post(
                endpoint,
                json=payload,  # Utilisation correcte de json=payload
                headers=self.headers
            )
            return response.json()
        except Exception as e:
            return {"error": str(e)}
    
    def get_results(self, task_id):
        endpoint = f"https://api.dataforseo.com/v3/serp/google/organic/task_get/{task_id}"
        try:
            response = requests.get(endpoint, headers=self.headers)
            return response.json()
        except Exception as e:
            return {"error": str(e)}


def scrape_google_urls(query, city, max_results=200, progress_bar=None):
    username = st.secrets["DATAFORSEO_USERNAME"]
    password = st.secrets["DATAFORSEO_PASSWORD"]
    api = DataForSEOAPI(username, password)
    
    # Création de la tâche
    post_response = api.post_task(f"{query} {city}")
    
    # Débogage
    st.write("Réponse POST de l'API:", post_response)
    
    if "tasks" not in post_response or not post_response["tasks"]:
        st.error(f"Erreur dans la création de la tâche: {post_response}")
        return None
    
    task_id = post_response["tasks"][0]["id"]
    
    # Paramètres pour la récupération des résultats
    max_attempts = 15  # Augmenté pour donner plus de temps à l'API
    wait_time = 45  # Temps d'attente initial
    
    # Attente et récupération des résultats
    for attempt in range(max_attempts):
        time.sleep(wait_time)
        
        # Après la première tentative, augmenter le temps d'attente
        if attempt > 0:
            wait_time = 30
        
        # Mise à jour de la barre de progression si fournie
        if progress_bar:
            progress_bar.progress((attempt + 1) / max_attempts)
        
        # Récupérer les résultats
        get_response = api.get_results(task_id)
        
        # Débogage
        st.write("Structure complète de la réponse:")
        st.json(get_response)
        
        # Vérification de la structure de la réponse
        if "tasks" in get_response and get_response["tasks"]:
            task = get_response["tasks"][0]
            if "result" in task and task["result"]:
                organic_results = []
                
                # Extraire les résultats organiques
                for item in task["result"]:
                    if "items" in item and item["items"]:
                        for organic_item in item["items"]:
                            if organic_item["type"] == "organic":
                                organic_results.append({
                                    "position": organic_item.get("rank_absolute", ""),
                                    "title": organic_item.get("title", ""),
                                    "url": organic_item.get("url", ""),
                                    "description": organic_item.get("description", ""),
                                    "city": city,
                                    "query": query
                                })
                
                return organic_results[:max_results]  # Limiter au nombre de résultats demandés
        else:
            st.write("Structure de réponse inattendue")
    
    # Si on arrive ici, c'est qu'on n'a pas réussi à récupérer les résultats
    st.error("Impossible de récupérer les résultats après plusieurs tentatives")
    return None


def extract_domain(url):
    match = re.search(r'https?://(?:www\.)?([^/]+)', url)
    if match:
        return match.group(1)
    return url


def process_results(results):
    if not results:
        return None
    
    # Création du DataFrame
    df = pd.DataFrame(results)
    
    # Ajout de la colonne domaine
    df['domain'] = df['url'].apply(extract_domain)
    
    return df


def main():
    st.title("🔍 Scraper Google Search via DataForSEO (Standard Queue)")
    
    # Test API si coché
    if st.sidebar.checkbox("Tester connexion API"):
        username = st.secrets["DATAFORSEO_USERNAME"]
        password = st.secrets["DATAFORSEO_PASSWORD"]
        api = DataForSEOAPI(username, password)
        
        test_response = requests.get(
            "https://api.dataforseo.com/v3/merchant/account_info", 
            headers=api.headers
        )
        st.sidebar.write("Test de connexion API:", test_response.status_code)
        st.sidebar.json(test_response.json())
        return
    
    # Entrée utilisateur
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("Entrez votre terme de recherche")
        query = st.text_input("", value="avocat", help="Terme de recherche (exemple: avocat)")
    
    with col2:
        st.write("Liste des villes (une par ligne)")
        cities_text = st.text_area("", value="Paris", height=120, help="Entrez une ville par ligne")
    
    cities = [city.strip() for city in cities_text.split("\n") if city.strip()]
    
    # Nombre de résultats par ville
    st.write("Nombre de résultats à récupérer par ville")
    max_results = st.slider("", 10, 200, 200, help="Maximum de résultats à récupérer par ville")
    
    # Bouton pour lancer le scraping
    if st.button("🔍 Lancer les recherches", type="primary"):
        if not query or not cities:
            st.error("Veuillez entrer un terme de recherche et au moins une ville")
            return
        
        all_results = []
        progress_bar = st.progress(0)
        
        for i, city in enumerate(cities):
            st.write(f"Recherche en cours pour : {query} {city} (Peut prendre jusqu'à 5 minutes)")
            results = scrape_google_urls(query, city, max_results, progress_bar)
            
            if results:
                all_results.extend(results)
                progress_bar.progress((i + 1) / len(cities))
            else:
                st.warning(f"Aucun résultat trouvé pour {query} {city}")
        
        if all_results:
            df = process_results(all_results)
            
            # Affichage des résultats
            st.success(f"{len(all_results)} résultats récupérés au total")
            st.dataframe(df)
            
            # Export CSV
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Télécharger les résultats (CSV)",
                csv,
                f"resultats_{query.replace(' ', '_')}.csv",
                "text/csv",
                key='download-csv'
            )
        else:
            st.error("Aucun résultat trouvé. Base toi sur cette docs pour essayer de corriger l'erreur : https://docs.dataforseo.com/v3/serp/google/organic/task_get/regular/?bash&_gl=1*12qovjf*_up*MQ..*_ga*MTcwODY0ODgwNC4xNzQzNDE5OTMx*_ga_T5NKP5Y695*MTc0MzQxOTkzMS4xLjEuMTc0MzQxOTk2OS4wLjAuMTA2MTE2MDU2MA..")


if __name__ == "__main__":
    main()
