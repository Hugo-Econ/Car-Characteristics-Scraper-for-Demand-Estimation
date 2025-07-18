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
from bs4 import BeautifulSoup # Webscrapping  
import time
import pprint
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed # Parallelizing  
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
def fetch_models_for_make(make, url):
    print(f"🔍 Fetching models for: {make}")
    try:
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "html.parser")

        # --- Extract Production models ---
        model_tags = soup.select('div.s h2.st:contains("Production models") + ul li a.e-a.e-t')
        if not model_tags:  # fallback if the above doesn't work
            model_tags = soup.select('ul.eg.eg-t1 li a.e-a.e-t')

        model_names = set(tag.text.strip().replace(f"{make} ", "") for tag in model_tags)

        # --- Extract Other models ---
        other_model_tags = soup.select('div.s h2.st:contains("Other models") + ul li a.txt')
        other_model_names = set(tag.text.strip().replace(f"{make} ", "") for tag in other_model_tags)

        all_models = sorted(model_names.union(other_model_names))

        if all_models:
            print(f"  ✅ Found {len(all_models)} models for {make}.")
            return (make, all_models)
        else:
            print(f"  ⚠️ No models found for {make}.")
            return (make, [])
    except Exception as e:
        print(f"  ❌ Failed to process {make}: {e}")
        return (make, [])

# 🚀 Run in parallel
make_to_models = {}
with ThreadPoolExecutor(max_workers=20) as executor:
    futures = [executor.submit(fetch_models_for_make, make, url) for make, url in make_links.items()]
    for future in as_completed(futures):
        make, models = future.result()
        if models:
            make_to_models[make] = models
        time.sleep(0.2)  # Light delay between completed futures to throttle output

# ✅ Preview result
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
    # print(f"🔍 Parsing: {year} - {trim}")
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "html.parser")

    return {
        "Year": year,
        "Trim": trim,
        "MSRP": get_text_or_none(soup, "MSRP"),
        "Engine": get_text_or_none(soup, "Engine"),
        "Power": get_text_or_none(soup, "Power"),
        #"Torque": get_text_or_none(soup, "Torque"),
        "Fuel_Cost": 0 if get_text_or_none(soup, "Engine") == "Electric" else get_text_or_none(soup, "Combined"),
        "Vehicle_Type": get_text_or_none(soup, "Vehicle type"),
        "Category": get_text_or_none(soup, "Category"),
        "Weight": get_text_or_none(soup, "Weight"),
        "Charging_time": get_text_or_none(soup, "Charging times"),
        "Range": get_text_or_none(soup, "Electric autonomy"),
        "Battery": get_text_or_none(soup, "Energy"),
        "Co2_km": get_text_or_none(soup, "CO₂ emissions"),
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


# %% Get maker-model-year-spec AND their key characteristic 
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Make sure these helper functions are defined beforehand:
# get_trim_urls_from_spec_page(...)
# parse_spec_page(...)

def process_make(make, models):
    print(f"\n🚗 Processing brand: {make}")
    all_specs = []

    for model in models:
        model_slug = model.lower().replace(" ", "-")
        start_url = f"{BASE_URL}/en/makes/{make.lower()}/{model_slug}/"

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
                            time.sleep(0.4)  # Still important to respect rate limits
                        except Exception as e:
                            print(f"❌ Error trim: {trim} → {e}")
                except Exception as e:
                    print(f"❌ Error year {year}: {e}")
        except Exception as e:
            print(f"❌ Could not process {make} {model}: {e}")

    if all_specs:
        df = pd.DataFrame(all_specs)
        print(f"✅ Finished {make} with {len(df)} rows.")
        return make, df
    else:
        print(f"⚠️ No data collected for {make}.")
        return make, None

brand_dataframes = {}
# subset_20 = dict(list(make_to_models.items())[:15])

with ThreadPoolExecutor(max_workers=18) as executor:  # You can adjust 5 to 10–20 if your machine can handle it
    futures = [executor.submit(process_make, make, models) for make, models in make_to_models.items()]
    
    for future in as_completed(futures):
        make, df = future.result()
        if df is not None:
            brand_dataframes[make] = df

print("\n✅ All brands processed. Previewing one:")
if 'Nissan' in brand_dataframes:
    print(brand_dataframes['Nissan'].head().T)

df_chevy = brand_dataframes.get('Chevrolet')


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
df_electric['Model'].unique()
test = df_electric[df_electric['Model']=="Bolt EV"]
print(df_electric.head())

conn.close()

