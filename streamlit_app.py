import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime
import logging
import io
import json
import base64

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataForSEOAPI:
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.post_url = "https://api.dataforseo.com/v3/serp/google/organic/task_post"
        self.get_url = "https://api.dataforseo.com/v3/serp/google/organic/task_get"
        self.auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.headers = {
            'Authorization': f'Basic {self.auth}',
            'Content-Type': 'application/json'
        }
    
    def post_task(self, query):
        """Soumet une tâche à la file d'attente DataForSEO"""
        data = [{
            "keyword": query,
            "location_name": "France",
            "language_name": "French",
            "device": "desktop",
            "os": "windows",
            "depth": 100,
            "se_domain": "google.fr"
        }]
        
        try:
            response = requests.post(self.post_url, headers=self.headers, data=json.dumps(data))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête POST API: {str(e)}")
            return None
    
    def get_results(self, task_id):
        """Récupère les résultats d'un ID de tâche spécifique"""
        try:
            response = requests.get(f"{self.get_url}/{task_id}", headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la requête GET API: {str(e)}")
            return None

def scrape_google_urls(query, max_results=200, progress_bar=None):
    """Scrape les résultats Google via DataForSEO en utilisant le mode queue standard"""
    results = []
    
    username = st.secrets["DATAFORSEO_USERNAME"]
    password = st.secrets["DATAFORSEO_PASSWORD"]
    api = DataForSEOAPI(username, password)
    
    if progress_bar:
        progress_bar.progress(0.1)  # Indiquer le début de la requête
    
    logger.info(f"Soumission de la tâche pour la requête: {query}")
    
    # Étape 1: Soumettre la tâche
    post_response = api.post_task(query)
    
    if not post_response or post_response.get('status_code') != 20000:
        error_message = post_response.get('status_message', 'Unknown error') if post_response else "Pas de réponse de l'API"
        logger.error(f"Erreur API (POST): {error_message}")
        return results
    
    # Récupérer l'ID de la tâche
    tasks = post_response.get('tasks', [])
    if not tasks:
        logger.error("Aucune tâche créée")
        return results
    
    task_id = tasks[0].get('id')
    if not task_id:
        logger.error("ID de tâche non trouvé")
        return results
    
    logger.info(f"Tâche soumise avec succès. ID: {task_id}")
    
    if progress_bar:
        progress_bar.progress(0.3)
    
    # Étape 2: Attendre puis récupérer les résultats (avec des tentatives)
    max_attempts = 10
    for attempt in range(max_attempts):
        if progress_bar:
            progress_value = 0.3 + (0.6 * (attempt / max_attempts))
            progress_bar.progress(progress_value)
        
        logger.info(f"Tentative {attempt+1}/{max_attempts} de récupérer les résultats...")
        
        # Attendre que la tâche soit traitée (5 minutes de délai selon l'image)
        wait_time = 30 if attempt == 0 else 60  # Attendre 30s la première fois, puis 60s
        time.sleep(wait_time)
        
        # Récupérer les résultats
        get_response = api.get_results(task_id)
        
        if not get_response:
            logger.warning("Pas de réponse lors de la récupération des résultats")
            continue
        
        if get_response.get('status_code') != 20000:
            error_message = get_response.get('status_message', 'Unknown error')
            logger.warning(f"Erreur API (GET): {error_message}")
            continue
        
        tasks = get_response.get('tasks', [])
        if not tasks:
            logger.warning("Aucune tâche retournée")
            continue
        
        # Vérifier si la tâche est terminée
        task_status = tasks[0].get('status_code')
        if task_status == 20000:  # Succès
            result_items = []
            task_result = tasks[0].get('result', [])
            
            if task_result:
                for item in task_result:
                    items = item.get('items', [])
                    result_items.extend(items)
            
            # Extraire les URLs des résultats
            for position, result in enumerate(result_items, start=1):
                if len(results) >= max_results:
                    break
                url = result.get('url')
                if url:
                    results.append({
                        "Position": position,
                        "URL": url
                    })
            
            logger.info(f"Scraping terminé. Nombre total de résultats: {len(results)}")
            break  # Sortir de la boucle si les résultats sont disponibles
        elif task_status == 40602:  # En attente
            logger.info("La tâche est toujours en attente de traitement...")
        else:
            logger.warning(f"Statut inattendu de la tâche: {task_status}")
    
    if progress_bar:
        progress_bar.progress(1.0)  # Compléter la barre de progression
    
    return results[:max_results]

def create_excel_with_multiple_sheets(dataframes, filename):
    """Crée un fichier Excel avec plusieurs onglets"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    return output.getvalue()

def main():
    st.title("🔍 Scraper Google Search via DataForSEO (Standard Queue)")
    
    if "DATAFORSEO_USERNAME" not in st.secrets or "DATAFORSEO_PASSWORD" not in st.secrets:
        st.error("Identifiants DataForSEO manquants. Veuillez configurer vos secrets Streamlit.")
        return
    
    # Liste des villes
    default_cities = """Paris
Paris 1er arrondissement
Paris 2e arrondissement
Paris 3e arrondissement
Paris 4e arrondissement
Paris 5e arrondissement
Paris 6e arrondissement
Paris 7e arrondissement
Paris 8e arrondissement
Paris 9e arrondissement
Paris 10e arrondissement
Paris 11e arrondissement
Paris 12e arrondissement
Paris 13e arrondissement
Paris 14e arrondissement
Paris 15e arrondissement
Paris 16e arrondissement
Paris 17e arrondissement
Paris 18e arrondissement
Paris 19e arrondissement
Paris 20e arrondissement
Marseille
Lyon
Toulouse
Nice
Nantes
Strasbourg
Montpellier
Bordeaux
Lille
Rennes
Reims
Saint-Etienne
Toulon
Le Havre
Grenoble
Dijon
Angers
Nimes
Villeurbanne
Clermont-Ferrand
Saint-Denis
Le Mans
Aix-en-Provence
Brest
Tours
Amiens
Limoges
Annecy
Perpignan
Boulogne-Billancourt
Metz
Besancon
Orleans
Saint-Denis
Rouen
Argenteuil
Mulhouse
Montreuil
Caen
Nancy
Saint-Paul
Roubaix
Tourcoing
Nanterre
Vitry-sur-Seine
Avignon
Creteil
Poitiers
Dunkerque
Asnieres-sur-Seine
Courbevoie
Versailles
Colombes
Fort-de-France
Aulnay-sous-Bois
Saint-Pierre
Rueil-Malmaison
Pau
Aubervilliers
Champigny-sur-Marne
Le Tampon
Antibes
Saint-Maur-des-Fosses
Cannes
Drancy
Merignac
Saint-Nazaire
Colmar
Issy-les-Moulineaux
Noisy-le-Grand
Evry-Courcouronnes
Levallois-Perret
Troyes
Neuilly-sur-Seine
Sarcelles
Venissieux
Clichy
Pessac
Ivry-sur-Seine
Cergy
Quimper
La Rochelle
Beziers
Ajaccio
Saint-Quentin
Niort
Villejuif
Hyeres
Pantin
Chambery
Le Blanc-Mesnil
Lorient
Les Abymes
Montauban
Sainte-Genevieve-des-Bois
Suresnes
Meaux
Valence
Beauvais
Cholet
Chelles
Bondy
Frejus
Clamart
Narbonne
Bourg-en-Bresse
Fontenay-sous-Bois
Bayonne
Sevran
Antony
Maisons-Alfort
La Seyne-sur-Mer
Epinay-sur-Seine
Montrouge
Saint-Herblain
Calais
Vincennes
Macon
Villepinte
Martigues
Bobigny
Cherbourg-en-Cotentin
Vannes
Massy
Brive-la-Gaillarde
Arles
Corbeil-Essonnes
Saint-Andre
Saint-Ouen-sur-Seine
Albi
Belfort
Evreux
La Roche-sur-Yon
Saint-Malo
Bagneux
Chateauroux
Noisy-le-Sec
Salon-de-Provence
Le Cannet
Vaulx-en-Velin
Livry-Gargan
Angouleme
Sete
Puteaux
Thionville
Rosny-sous-Bois
Saint-Laurent-du-Maroni
Alfortville
Istres
Gennevilliers
Wattrelos
Talence
Blois
Tarbes
Castres
Garges-les-Gonesse
Saint-Brieuc
Arras
Douai
Compiegne
Melun
Reze
Saint-Chamond
Bourgoin-Jallieu
Gap
Montelimar
Thonon-les-Bains
Draguignan
Chartres
Joue-les-Tours
Saint-Martin-dHeres
Villefranche-sur-Saone
Chalon-sur-Saone
Mantes-la-Jolie
Colomiers
Anglet
Pontault-Combault
Poissy
Savigny-sur-Orge
Bagnolet
Lievin
Nevers
Gagny
Le Perreux-sur-Marne
Stains
Chalons-en-Champagne
Conflans-Sainte-Honorine
Montlucon
Palaiseau
Laval
Saint-Priest
LHay-les-Roses
Brunoy
Chatillon
Sainte-Marie
Bastia
Lens
Chambery
Saint-Benoit
Le Port
Saint-Leu
Noumea"""
    
    # Interface utilisateur
    with st.container():
        col1, col2 = st.columns(2)
        
        with col1:
            query = st.text_input(
                "Entrez votre terme de recherche",
                value="",
                placeholder="Exemple: avocat",
                help="Tapez votre terme de recherche principal"
            )
        
        with col2:
            cities = st.text_area(
                "Liste des villes (une par ligne)",
                value=default_cities,
                height=100,
                help="Entrez les villes, une par ligne"
            )
        
        max_results = st.select_slider(
            "Nombre de résultats à récupérer par ville",
            options=[10, 20, 30, 50, 100, 200],
            value=100,
            help="Choisissez le nombre de résultats Google à récupérer par ville"
        )
        
        # Calcul des coûts
        cities_list = [city.strip() for city in cities.split('\n') if city.strip()]
        total_requests = len(cities_list)
        cost_per_request = 0.0006  # $0.0006 par page SERP (Standard Queue)
        estimated_cost = total_requests * cost_per_request
        
        # Informations de coût dans la sidebar
        st.sidebar.title("Estimation des coûts")
        st.sidebar.write(f"Nombre de villes: {len(cities_list)}")
        st.sidebar.write(f"Total requêtes: {total_requests}")
        st.sidebar.write(f"Coût estimé: ${estimated_cost:.4f}")
        st.sidebar.write("Mode: Standard Queue ($0.0006/page)")
        
        search_button = st.button("🔍 Lancer les recherches")
        
        if search_button:
            if not query or not cities_list:
                st.error("Veuillez entrer un terme de recherche et au moins une ville")
                return
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Dictionnaire pour stocker les résultats par ville
            all_results = {}
            
            for i, city in enumerate(cities_list):
                full_query = f"{query} {city}"
                status_text.text(f"Recherche en cours pour : {full_query} (Peut prendre jusqu'à 5 minutes)")
                
                data = scrape_google_urls(full_query, max_results, progress_bar)
                if data:
                    df = pd.DataFrame(data)[["Position", "URL"]]
                    all_results[full_query] = df
                
                progress = (i + 1) / len(cities_list)
                progress_bar.progress(progress)
                
                # Pas besoin d'un délai supplémentaire car le mode queue a déjà un délai intégré
                
            if all_results:
                st.success(f"Recherches terminées ! Résultats trouvés pour {len(all_results)} villes.")
                
                # Création du fichier Excel
                excel_data = create_excel_with_multiple_sheets(all_results, "resultats_recherche.xlsx")
                
                # Bouton de téléchargement
                st.download_button(
                    label="📥 Télécharger les résultats (Excel)",
                    data=excel_data,
                    file_name=f"recherche_{query}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
                # Affichage des aperçus
                for query_city, df in all_results.items():
                    with st.expander(f"Aperçu des résultats pour : {query_city}"):
                        st.dataframe(df)
                
                # Statistiques dans la sidebar
                st.sidebar.write("---")
                st.sidebar.write("Statistiques de la recherche")
                st.sidebar.write(f"Villes traitées: {len(all_results)}")
                st.sidebar.write(f"Coût réel: ${(len(all_results) * cost_per_request):.4f}")
            else:
                st.error("Aucun résultat trouvé.")

if __name__ == "__main__":
    main()
