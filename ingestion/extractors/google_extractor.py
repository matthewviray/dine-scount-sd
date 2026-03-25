import os
import requests
import json
import hashlib
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')

NEIGHBORHOODS = [
    # Central
    "North Park, San Diego",
    "Hillcrest, San Diego",
    "South Park, San Diego",
    "Normal Heights, San Diego",
    "City Heights, San Diego",

    # Downtown
    "Little Italy, San Diego",
    "Gaslamp Quarter, San Diego",
    "East Village, San Diego",
    "Barrio Logan, San Diego",
    "Old Town, San Diego",

    # Coastal
    "Pacific Beach, San Diego",
    "Mission Beach, San Diego",
    "Ocean Beach, San Diego",
    "La Jolla, San Diego",
    "Coronado, San Diego",

    # North
    "Mira Mesa, San Diego",
    "Rancho Bernardo, San Diego",
    "Carmel Valley, San Diego",
    "Del Mar, San Diego",
    "Encinitas, San Diego",

    # East
    "El Cajon, San Diego",
    "La Mesa, San Diego",
    "Santee, San Diego",

    # South
    "Chula Vista, San Diego",
    "National City, San Diego",

    # Food Corridors
    "Convoy, San Diego",
]

SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
INCLUDED_TYPES = ["restaurant"]


def search_resturants(neighborhood, included_type="restaurant"):
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.rating,places.userRatingCount,places.priceLevel,places.formattedAddress,places.types,places.location,places.regularOpeningHours,places.photos,places.websiteUri"
        
        

    }

    body = {
        "textQuery": f"food in {neighborhood}",
        "maxResultCount": 20,
        "includedType": included_type
    }

    response = requests.post(SEARCH_URL, headers=headers, json=body)
    response.raise_for_status()
    return response.json().get("places",[])

def extract_restaurants():
    restaurants = []
    place_ids = set()
    for neighborhood in NEIGHBORHOODS:
        for included_types in INCLUDED_TYPES:
            try:
                results = search_resturants(neighborhood, included_type=included_types)
                for place in results:
                    place_id = place.get("id")
                    if place_id and place_id not in place_ids:
                        place_ids.add(place["id"])
                        restaurants.append({
                            "place_id": place_id,
                            "name": place.get("displayName", {}).get("text", ""),
                            "rating": place.get("rating"),
                            "review_count": place.get("userRatingCount"),
                            "price_level": place.get("priceLevel"),
                            "website": place.get("websiteUri",""),
                            "photo_url":  place.get("photos",[{}])[0].get("name",""),
                            "hours": json.dumps(place.get("regularOpeningHours",{})),
                            "address": place.get("formattedAddress", ""),
                            "types": json.dumps(place.get("types", [])),
                            "lat": place.get("location", {}).get("latitude"),
                            "lng": place.get("location", {}).get("longitude"),
                            "neighborhood": neighborhood,
                            "ingested_at": datetime.utcnow().isoformat()

                        })
            except Exception as e:
                print(f"Error searching for resturants in {neighborhood}: {e}")
    print(f"Total unique places fetched: {len(restaurants)}")
    return restaurants

if __name__ == "__main__":
    results = extract_restaurants()
    print(results[0] if results else "No results")
