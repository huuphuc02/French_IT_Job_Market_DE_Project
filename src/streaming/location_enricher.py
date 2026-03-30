import re


def parse_location(lieu_travail: dict) -> dict:
    libelle     = lieu_travail.get("libelle", "") or ""
    code_postal = lieu_travail.get("codePostal")

    # Extract dept code: "67 - STRASBOURG" → "67"
    m = re.match(r"^(\d{2,3}[AB]?)\s*[-–]", libelle.strip())
    dept_code    = m.group(1) if m else (code_postal[:2] if code_postal else None)

    # Extract city name: "67 - STRASBOURG" → "Strasbourg"
    commune_name = libelle.split(" - ", 1)[1].title() if " - " in libelle else None

    return {
        "commune_code":  lieu_travail.get("commune"),
        "commune_name":  commune_name,
        "code_postal":   code_postal,
        "dept_code":     dept_code,    
        "latitude":      lieu_travail.get("latitude"),
        "longitude":     lieu_travail.get("longitude"),
    }