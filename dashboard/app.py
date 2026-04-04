
import os
import streamlit as st
import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "http://localhost:8000"


st.set_page_config(
    page_title="RestaurantPulse SD",
    page_icon="🍽️",
    layout="wide"
)

st.markdown("""
<style>
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.78rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-best { background: #3a2f00; color: #f5c518; }
    .badge-hot  { background: #3a1a00; color: #ff6b35; }
    .badge-gem  { background: #002b3a; color: #4fc3f7; }
    .badge-new  { background: #1a2f00; color: #81c784; }
    img { border-radius: 10px; }
    .hours-table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    .hours-table tr:nth-child(even) { background: rgba(0,0,0,0.05); }
    .hours-table td { padding: 5px 14px; color: #000000; }
    .hours-day { font-weight: 600; width: 110px; }
</style>
""", unsafe_allow_html=True)

# HELPER FUNCTIONS

def fetch_recommendations(category, neighborhood=None, cuisine=None, price=None, limit=10):
    params = {"category": category, "limit": limit}
    if neighborhood:
        params["neighborhood"] = neighborhood
    if cuisine:
        params["cuisine"] = cuisine
    if price:
        params["price"] = price
    try:
        response = requests.get(f"{API_URL}/recommendations", params=params)
        return response.json()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return []

def fetch_neighborhoods():
    try:
        response = requests.get(f"{API_URL}/neighborhoods")
        return response.json()
    except:
        return []

def get_photo_url(photo_resource_name):
    if not photo_resource_name:
        return None
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    return f"https://places.googleapis.com/v1/{photo_resource_name}/media?key={api_key}&maxHeightPx=400"

def price_label(price_level):
    mapping = {1: "$", 2: "$$", 3: "$$$", 4: "$$$$"}
    return mapping.get(price_level, "N/A")

def display_restaurant_card(restaurant):
    import json, re

    badge_html = ""
    if restaurant.get("is_best_overall"):
        badge_html += '<span class="badge badge-best">🏆 Best Overall</span>'
    if restaurant.get("is_hot_right_now"):
        badge_html += '<span class="badge badge-hot">🔥 Hot Right Now</span>'
    if restaurant.get("is_hidden_gem"):
        badge_html += '<span class="badge badge-gem">💎 Hidden Gem</span>'
    if restaurant.get("is_new_spot"):
        badge_html += '<span class="badge badge-new">🆕 New Spot</span>'

    col1, col2 = st.columns([1, 2])

    with col1:
        photo_url = get_photo_url(restaurant.get("photo_url"))
        if photo_url:
            st.image(photo_url, use_container_width=True)
        else:
            st.image("https://via.placeholder.com/400x300?text=No+Photo", use_container_width=True)

    with col2:
        st.markdown(f"### {restaurant.get('name')}")
        st.markdown(f"⭐ {restaurant.get('rating')} · {restaurant.get('review_count')} reviews · {price_label(restaurant.get('price_level'))}")
        st.markdown(f"📍 {restaurant.get('neighborhood')} · {restaurant.get('address')}")

        if badge_html:
            st.markdown(badge_html, unsafe_allow_html=True)

        if restaurant.get("website"):
            st.markdown(f"[🌐 Visit Website]({restaurant.get('website')})")

        st.markdown("**Scores**")
        st.progress(min((restaurant.get("rating_score") or 0) / 10, 1.0), text=f"⭐ Rating: {restaurant.get('rating_score')}")
        st.progress(min((restaurant.get("popularity_score") or 0) / 10, 1.0), text=f"👥 Popularity: {restaurant.get('popularity_score')}")
        st.progress(min((restaurant.get("hidden_gem_score") or 0) / 10, 1.0), text=f"💎 Hidden Gem: {restaurant.get('hidden_gem_score')}")

        hours = restaurant.get("hours")
        if hours:
            with st.expander("🕐 Hours"):
                if isinstance(hours, str):
                    try:
                        hours = json.loads(hours)
                    except Exception:
                        hours = [hours]
                rows = ""
                for day in hours:
                    match = re.split(r":\s+|\s{2,}", day.strip(), maxsplit=1)
                    day_name = match[0] if len(match) == 2 else day
                    time_val = match[1] if len(match) == 2 else ""
                    rows += f'<tr><td class="hours-day">{day_name}</td><td>{time_val}</td></tr>'
                st.markdown(f'<table class="hours-table">{rows}</table>', unsafe_allow_html=True)

    st.divider()

# SIDEBAR FILTERS


st.sidebar.title("🍽️ RestaurantPulse SD")
st.sidebar.markdown("Discover the best restaurants in San Diego")
st.sidebar.divider()

neighborhoods = fetch_neighborhoods()
neighborhood_options = ["All Neighborhoods"] + neighborhoods
selected_neighborhood = st.sidebar.selectbox("📍 Neighborhood", neighborhood_options)
if selected_neighborhood == "All Neighborhoods":
    selected_neighborhood = None

cuisine_options = ["All Cuisines", "mexican", "vietnamese", "japanese", "chinese",
                   "korean", "italian", "indian", "thai", "mediterranean",
                   "middle_eastern", "american", "seafood", "pizza", "burger",
                   "sushi", "ramen", "vegan", "vegetarian", "breakfast", "cafe"]
selected_cuisine = st.sidebar.selectbox("🍜 Cuisine", cuisine_options)
if selected_cuisine == "All Cuisines":
    selected_cuisine = None

price_options = {"Any Price": None, "$": 1, "$$": 2, "$$$": 3, "$$$$": 4}
selected_price_label = st.sidebar.selectbox("💰 Price", list(price_options.keys()))
selected_price = price_options[selected_price_label]

st.sidebar.divider()
st.sidebar.markdown("*Data updated weekly from Google Places*")

# ─────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────

st.title("🍽️ RestaurantPulse SD")
st.markdown("San Diego's smartest restaurant recommendations — powered by real data.")

with st.expander("📊 How are scores calculated?"):
    st.markdown("""
| Score | Formula | What it means |
|---|---|---|
| ⭐ Rating | `rating × 2` (max 10) | Raw Google rating scaled to 10 |
| 👥 Popularity | `(review_count ÷ neighborhood avg) × 5`, capped at 10 | How popular vs nearby restaurants |
| 🚀 Velocity | Sum of rating changes over last 30 days `× 2` | How fast the rating is climbing |
| 💎 Hidden Gem | `rating ÷ log(review_count + 1)` | High quality, low exposure |
""")

with st.expander("🏷️ How are categories determined?"):
    st.markdown("""
| Category | Requirements |
|---|---|
| 🏆 Best Overall | Rating ≥ 4.0 **and** review count ≥ neighborhood average |
| 🔥 Hot Right Now | Rating has increased at least once in the last 30 days |
| 💎 Hidden Gem | Hidden gem score ≥ 1.8 **and** at least 30 reviews — high quality, low exposure |
| 🆕 New Spot | First seen in our system within the last 6 months **and** rating ≥ neighborhood average |
""")
    st.caption("⚠️ Note: Google Places does not provide an official opening date for restaurants. New Spot is based on when a restaurant first appeared in our weekly data collection — it's a proxy, not an exact open date.")

st.divider()

tab1, tab2, tab3, tab4 = st.tabs(["🏆 Best Overall", "🔥 Hot Right Now", "💎 Hidden Gems", "🆕 New Spots"])

with tab1:
    st.markdown("Highest rated restaurants with strong review counts")
    results = fetch_recommendations("best_overall", neighborhood=selected_neighborhood, cuisine=selected_cuisine, price=selected_price, limit=5)
    if results:
        for restaurant in results:
            display_restaurant_card(restaurant)
    else:
        st.info("No restaurants found for the selected filters.")

with tab2:
    st.markdown("Ratings climbing fastest over the last 30 days")
    results = fetch_recommendations("hot_right_now", neighborhood=selected_neighborhood, cuisine=selected_cuisine, price=selected_price, limit=5)
    if results:
        for restaurant in results:
            display_restaurant_card(restaurant)
    else:
        st.info("No restaurants found for the selected filters.")

with tab3:
    st.markdown("High rated but undiscovered — low reviews, high quality")
    results = fetch_recommendations("hidden_gems", neighborhood=selected_neighborhood, cuisine=selected_cuisine, price=selected_price, limit=5)
    if results:
        for restaurant in results:
            display_restaurant_card(restaurant)
    else:
        st.info("No restaurants found for the selected filters.")

with tab4:
    st.markdown("Recently opened and already above average")
    results = fetch_recommendations("new_spots", neighborhood=selected_neighborhood, cuisine=selected_cuisine, price=selected_price, limit=5)
    if results:
        for restaurant in results:
            display_restaurant_card(restaurant)
    else:
        st.info("No restaurants found for the selected filters.")