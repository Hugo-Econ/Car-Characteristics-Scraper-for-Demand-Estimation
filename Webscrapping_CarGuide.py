"""
Created on Mon Jul 14 14:19:26 2025
Scrapes and save in SQL detailed car specifications (e.g., engine type, MSRP, power, weight) by 
make-model-year-trim from GuideAutoWeb. Outputs a clean, structured SQLite database to support car-level 
demand estimation.
@author: Hugo
"""
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import pprint
import sqlite3

ev_makers = [
    "Acura", "Alfa Romeo", "Allard", "Aston Martin", "Audi",
    "Bentley", "BMW", "Bugatti", "Buick", "Byd",
    "Cadillac", "Campagna Motors", "Chevrolet", "Chrysler",
    "Dodge",
    "Faraday Future", "Felino", "Ferrari", "Fiat", "Fisker", "Ford",
    "Genesis", "GMC",
    "Honda", "Hummer", "Hyundai",
    "Ineos", "Infiniti", "Isuzu",
    "Jaguar", "Jeep",
    "Karma", "Kia", "Koenigsegg",
    "Lamborghini", "Lancia", "Land Rover", "Lexus", "Lincoln", "Liteborne", "Lotus", "Lucid",
    "Maserati", "Maybach", "Mazda", "McLaren", "Mercedes-Benz", "Mercury", "MINI", "Mitsubishi",
    "Nissan",
    "Oldsmobile", "Opel",
    "Pagani", "Panoz", "Peugeot", "Polestar", "Pontiac", "Porsche",
    "Ram", "Renault", "Rimac", "Rivian", "Rolls-Royce",
    "Saab", "Saleen", "Saturn", "Scion", "smart", "Spyker", "SRT", "Subaru", "Suzuki",
    "Tata", "Tesla", "Toyota",
    "VinFast", "Volkswagen", "Volvo"
]

BASE_URL = "https://www.guideautoweb.com"
MAKE_LIST_URL = f"{BASE_URL}/en/makes/"


# %% Get make_to_models
# Step 1: Get all available makes
response = requests.get(MAKE_LIST_URL)
soup = BeautifulSoup(response.text, "html.parser")

make_links = {
    a.text.strip(): BASE_URL + a['href']
    for a in soup.select("ul#brands-index-list li a")
}

# Step 2: For each make, extract production model names
make_to_models = {}

for make, url in make_links.items():
    print(f"🔍 Fetching models for: {make}")
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        # Find the model names under the "Production models" section
        model_tags = soup.select('div.s h2.st:contains("Production models") + ul li a.e-a.e-t')
        if not model_tags:  # fallback if the above doesn't work
            model_tags = soup.select('ul.eg.eg-t1 li a.e-a.e-t')

        model_names = sorted(set(tag.text.strip().replace(f"{make} ", "") for tag in model_tags))
        if model_names:
            make_to_models[make] = model_names
            print(f"  ✅ Found {len(model_names)} models.")
        else:
            print(f"  ⚠️ No models found.")
        time.sleep(1)
    except Exception as e:
        print(f"  ❌ Failed to process {make}: {e}")

# Preview result
for k, v in list(make_to_models.items())[:5]:
    print(f"\n{k}: {v}")
    
 
    

# %% Get maker-model-year-spec AND their key characteristic 
def get_text_or_none(soup, label):
    row = soup.find("th", string=label)
    if row:
        value_cell = row.find_next_sibling("td")
        if value_cell:
            return value_cell.text.strip()
    return None

def parse_spec_page(url, year, trim):
    print(f"🔍 Parsing: {year} - {trim}")
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    return {
        "Year": year,
        "Trim": trim,
        "MSRP": get_text_or_none(soup, "MSRP"),
        "Engine": get_text_or_none(soup, "Engine"),
        "Power": get_text_or_none(soup, "Power"),
        "Torque": get_text_or_none(soup, "Torque"),
        "Fuel_Cost": 0 if get_text_or_none(soup, "Engine") == "Electric" else get_text_or_none(soup, "Combined"),
        "Vehicle Type": get_text_or_none(soup, "Vehicle type"),
        "Category": get_text_or_none(soup, "Category"),
        "Weight": get_text_or_none(soup, "Weight"),
        "URL": url
    }

def get_trim_urls_from_spec_page(spec_page_url):
    r = requests.get(spec_page_url)
    soup = BeautifulSoup(r.text, "html.parser")

    trims = {}
    trim_select = soup.find("select", {"name": "trim"})
    if trim_select:
        for opt in trim_select.find_all("option"):
            trim_name = opt.text.split(" - ")[0].strip()
            full_url = BASE_URL + opt['value']
            trims[trim_name] = full_url
    return trims

brand_dataframes = {}
save_dir = r"C:\Users\Hugo\Dropbox\1. School\1.Research\Optimal spatial fast charging policies\Data\clean\CarGuide"
os.makedirs(save_dir, exist_ok=True)

for make, models in make_to_models.items():
    print(f"\n🚗 Processing brand: {make}")
    all_specs = []
    
    for model in models:
        model_slug = model.lower().replace(" ", "-")
        start_url = f"{BASE_URL}/en/makes/{make.lower()}/{model_slug}/2025/"
        
        try:
            r = requests.get(start_url)
            soup = BeautifulSoup(r.text, "html.parser")
            year_select = soup.find("select", {"name": "year"})
            if not year_select:
                print(f"⚠️ No years found for {make} {model}")
                continue

            year_options = year_select.find_all("option")
            year_urls = {
                opt.text.strip(): BASE_URL + opt["value"]
                for opt in year_options
                if int(opt.text.strip()) >= 2012
            }

            for year, year_url in year_urls.items():
                print(f"\n🔎 {make} {model} — Year {year}")
                try:
                    r_year = requests.get(year_url)
                    soup_year = BeautifulSoup(r_year.text, "html.parser")

                    spec_link = soup_year.find("a", string="Specifications")
                    if not spec_link:
                        print(f"⚠️ No spec page for {year}")
                        continue

                    spec_url = BASE_URL + spec_link['href']
                    trim_urls = get_trim_urls_from_spec_page(spec_url)

                    for trim, url in trim_urls.items():
                        try:
                            spec_data = parse_spec_page(url, year, trim)
                            spec_data["Make"] = make
                            spec_data["Model"] = model
                            all_specs.append(spec_data)
                            time.sleep(0.4)
                        except Exception as e:
                            print(f"❌ Error trim: {trim} → {e}")
                except Exception as e:
                    print(f"❌ Error year {year}: {e}")
        except Exception as e:
            print(f"❌ Could not process {make} {model}: {e}")

    # Add to brand dictionary if any data collected
    if all_specs:
        brand_df = pd.DataFrame(all_specs)
        brand_dataframes[make] = brand_df
        print(f"✅ Finished {make} with {len(brand_df)} rows.")
    else:
        print(f"⚠️ No data collected for {make}.")

# ✅ Done!
print("\n✅ All brands processed. Previewing one:")
print(brand_dataframes['Nissan'].head().T)

# %% Save to SQLite under a single table
save_dir = r"C:\Users\Hugo\Dropbox\1. School\1.Research\Optimal spatial fast charging policies\Data\clean\CarGuide"
os.makedirs(save_dir, exist_ok=True)
db_path = os.path.join(save_dir, "ev_specs.sqlite")

# Combine all dataframes with a "Make" column
all_cars = []
for brand, df in brand_dataframes.items():
    df = df.copy()
    df["Make"] = brand  # Ensure 'Make' column is consistent
    all_cars.append(df)

combined_df = pd.concat(all_cars, ignore_index=True)

combined_df['Engine'].unique()


conn = sqlite3.connect(db_path)
combined_df.to_sql("Car_Guide", conn, index=False, if_exists='replace')
conn.close()

print(f"✅ All brand data saved to one table 'Car_Guide' at:\n{db_path}")

#Verification.

conn = sqlite3.connect(db_path)

query = """
SELECT *
FROM Car_Guide
WHERE LOWER(Engine) LIKE '%electric%'
"""

df_electric = pd.read_sql(query, conn)
df_electric.columns
print(df_electric.head())

conn.close()
