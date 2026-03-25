import os
from dotenv import load_dotenv

load_dotenv()

FT_TOKEN_URL = os.getenv("FT_TOKEN_URL", "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=%2Fpartenaire")
FT_OFFERS_URL = os.getenv("FT_OFFERS_URL", "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search")
FT_CLIENT_ID = os.getenv("FT_CLIENT_ID")
FT_CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET")
FT_SCOPE = os.getenv("FT_SCOPE", "api_offresdemploiv2 o2dsoffre")

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_TOPIC_BACKFILL = 'it-job-offers-backfill'

REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_PASSWORD = None

ROME_CODES = [
    "M1801", # Administrateur / Administratrice réseau informatique
    "M1802", # Ingénieur / Ingénieure système informatique
    "M1803", # Directeur / Directrice des services informatiques -DSI-
    "M1804", # Ingénieur / Ingénieure télécoms
    "M1805", # Développeur / Développeuse web
    "M1806", # Consultant / Consultant(e) en informatique
    "M1810", # Technicien / Technicienne informatique
    "M1811", # Data Engineer
    "M1812", # Responsable de la Sécurité des Systèmes d'Information (RSSI)
    "M1813", # Intégrateur / Intégratrice logiciels métiers
    "M1405", # Data scientist
    "E1206" # UI - UX Designer
]

PAGE_SIZE = 150 # max allowed by the API
REQUEST_DELAY = 0.2 # delay between paginated calls

