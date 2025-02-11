import os
import re
import requests
import sys
import json

# -------------------------------
# CONFIGURATION
# -------------------------------
BASE_FOLDER = r"J:\VM"  # Folder containing actor folders
STASH_API = "http://localhost:9999/graphql"
PER_PAGE = 2222  # High enough to return all results
STASH_HEADERS = {"Content-Type": "application/json"}

# FansDB configuration
FANSDB_API = "https://fansdb.cc/graphql"

issues_log = []
LOG_FILE = "issues.log"

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------
def log_issue(message):
    issues_log.append(message)
    print(f"LOG: {message}")  # Also print to console for debugging.
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    except Exception as e:
        print(f"Could not write to log file: {e}")

def clean_folder_name(name):
    return re.sub(r'[_-]\d+$', '', name).strip()

def transform_for_search(value):
    return value.replace(" ", "_").lower()

def stash_graphql(query, variables={}):
    payload = {"query": query, "variables": variables}
    try:
        response = requests.post(STASH_API, json=payload, headers=STASH_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log_issue(f"Stash GraphQL error: {e} with payload: {json.dumps(payload)}")
        raise

def fansdb_graphql(query, variables={}):
    payload = {"query": query, "variables": variables}
    try:
        response = requests.post(FANSDB_API, json=payload, headers=FANSDB_HEADERS, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        log_issue(f"FansDB GraphQL error: {e} with payload: {json.dumps(payload)}")
        raise

# -------------------------------
# FIND LOCAL PERFORMER (case-insensitive)
# -------------------------------
def find_performer(performer_name):
    query = (
        "query FindPerformers($filter: FindFilterType, $performer_filter: PerformerFilterType, $performer_ids: [Int!]) {"
        "\n  findPerformers(filter: $filter, performer_filter: $performer_filter, performer_ids: $performer_ids) {"
        "\n    count"
        "\n    performers {"
        "\n      id"
        "\n      name"
        "\n      alias_list"
        "\n      __typename"
        "\n    }"
        "\n    __typename"
        "\n  }"
        "\n}"
    )
    variables = {
        "filter": {
            "q": performer_name.lower(),
            "page": 1,
            "per_page": 40,
            "sort": "name",
            "direction": "ASC"
        },
        "performer_filter": {},
        "performer_ids": []
    }
    result = stash_graphql(query, variables)
    data = result.get("data", {}).get("findPerformers", {})
    return data.get("count", 0), data.get("performers", [])

# -------------------------------
# SCRAPE & CREATE PERFORMER FROM FANSDB
# -------------------------------
def scrape_and_create_performer(query_str):
    scrape_query = """query ScrapeSinglePerformer($source: ScraperSourceInput!, $input: ScrapeSinglePerformerInput!) {
  scrapeSinglePerformer(source: $source, input: $input) {
    ...ScrapedPerformerData
    __typename
  }
}
fragment ScrapedPerformerData on ScrapedPerformer {
  stored_id
  name
  disambiguation
  gender
  urls
  birthdate
  ethnicity
  country
  eye_color
  height
  measurements
  fake_tits
  penis_length
  circumcised
  career_length
  tattoos
  piercings
  aliases
  tags {
    ...ScrapedSceneTagData
    __typename
  }
  images
  details
  death_date
  hair_color
  weight
  remote_site_id
  __typename
}
fragment ScrapedSceneTagData on ScrapedTag {
  stored_id
  name
  __typename
}"""
    variables = {
        "source": {"stash_box_endpoint": "https://fansdb.cc/graphql"},
        "input": {"query": query_str}
    }
    try:
        result = stash_graphql(scrape_query, variables)
    except Exception as e:
        log_issue(f"Error scraping performer '{query_str}': {e}")
        return None

    if "errors" in result:
        log_issue(f"FansDB GraphQL error: {result['errors']} with payload: {json.dumps({'query': scrape_query, 'variables': variables})}")
        return None

    scraped = result.get("data", {}).get("scrapeSinglePerformer", [])
    if not scraped:
        log_issue(f"No performer data scraped for query '{query_str}'.")
        return None

    scraped_data = scraped[0]
    # Only add female performers.
    if scraped_data.get("gender", "").upper() != "FEMALE":
        log_issue(f"Scraped performer '{scraped_data.get('name')}' is not female. Skipping.")
        return None

    # Print outcome for debugging.
    print(f"FansDB scrape found performer: {scraped_data.get('name')}")

    try:
        height_str = scraped_data.get("height") or "0"
        height_cm = int(height_str)
    except ValueError:
        height_cm = 0

    performer_input = {
        "name": scraped_data.get("name", ""),
        "disambiguation": scraped_data.get("disambiguation", "") or "",
        "alias_list": [alias.strip() for alias in scraped_data.get("aliases", "").split(",") if alias.strip()],
        "gender": scraped_data.get("gender", ""),
        "birthdate": scraped_data.get("birthdate", ""),
        "death_date": scraped_data.get("death_date", "") or "",
        "country": scraped_data.get("country", ""),
        "ethnicity": scraped_data.get("ethnicity", ""),
        "hair_color": scraped_data.get("hair_color", ""),
        "eye_color": scraped_data.get("eye_color", ""),
        "height_cm": height_cm,
        "weight": scraped_data.get("weight", None),
        "measurements": scraped_data.get("measurements", ""),
        "fake_tits": scraped_data.get("fake_tits", ""),
        "penis_length": scraped_data.get("penis_length", None),
        "circumcised": scraped_data.get("circumcised", None),
        "tattoos": scraped_data.get("tattoos", ""),
        "piercings": scraped_data.get("piercings", ""),
        "career_length": scraped_data.get("career_length", ""),
        "urls": scraped_data.get("urls", []),
        "details": scraped_data.get("details", ""),
        "tag_ids": [],
        "ignore_auto_tag": False,
        "stash_ids": [{
            "endpoint": "https://fansdb.cc/graphql",
            "stash_id": scraped_data.get("remote_site_id", "")
        }],
        "image": scraped_data.get("images", [None])[0]
    }

    create_query = """mutation PerformerCreate($input: PerformerCreateInput!) {
  performerCreate(input: $input) {
    id
    name
    __typename
  }
}"""
    create_variables = {"input": performer_input}
    try:
        create_result = stash_graphql(create_query, create_variables)
    except Exception as e:
        log_issue(f"Error creating performer '{query_str}': {e}")
        return None

    new_perf = create_result.get("data", {}).get("performerCreate", {})
    if new_perf and "id" in new_perf:
        print(f"Successfully created performer '{new_perf.get('name')}' with ID {new_perf.get('id')}")
        return new_perf.get("id")
    else:
        log_issue(f"Failed to create performer from scraped data for '{query_str}'. Response: {create_result}")
        return None

# -------------------------------
# SCENE QUERY (FindScenes)
# -------------------------------
def find_scene_ids(search_value, performer_id):
    query = (
        "query FindScenes($filter: FindFilterType, $scene_filter: SceneFilterType, $scene_ids: [Int!]) {"
        "\n  findScenes(filter: $filter, scene_filter: $scene_filter, scene_ids: $scene_ids) {"
        "\n    scenes { id performers { id } __typename }"
        "\n    __typename"
        "\n  }"
        "\n}"
    )
    variables = {
        "filter": {"q": "", "page": 1, "per_page": PER_PAGE, "sort": "date", "direction": "DESC"},
        "scene_filter": {"path": {"value": transform_for_search(search_value), "modifier": "INCLUDES"}},
        "scene_ids": []
    }
    result = stash_graphql(query, variables)
    scenes = result.get("data", {}).get("findScenes", {}).get("scenes", [])
    return [scene["id"] for scene in scenes if performer_id not in [p["id"] for p in scene.get("performers", [])]]

# -------------------------------
# GALLERY-BASED IMAGE QUERY WITH ALIAS SEARCHES (CASE-INSENSITIVE)
# -------------------------------
def find_gallery_id_by_term(term):
    query = (
        "query FindGalleries($filter: FindFilterType, $gallery_filter: GalleryFilterType) {"
        "\n  findGalleries(gallery_filter: $gallery_filter, filter: $filter) {"
        "\n    count"
        "\n    galleries { id folder { path } __typename }"
        "\n    __typename"
        "\n  }"
        "\n}"
    )
    variables = {
        "filter": {"q": term.lower(), "page": 1, "per_page": 40, "sort": "path", "direction": "ASC"},
        "gallery_filter": {}
    }
    result = stash_graphql(query, variables)
    galleries = result.get("data", {}).get("findGalleries", {}).get("galleries", [])
    matches = [g for g in galleries if os.path.basename(g.get("folder", {}).get("path", "")).lower() == term.lower()]
    if len(matches) == 1:
        print(f"Gallery found for search term '{term}': ID {matches[0]['id']}")
        return matches[0]["id"]
    elif len(matches) == 0:
        log_issue(f"No gallery found for search term '{term}'.")
    else:
        log_issue(f"Multiple galleries found for search term '{term}'.")
    return None

def find_images_from_gallery(gallery_id, target_performer_id):
    query = (
        "query FindImages($filter: FindFilterType, $image_filter: ImageFilterType, $image_ids: [Int!]) {"
        "\n  findImages(filter: $filter, image_filter: $image_filter, image_ids: $image_ids) {"
        "\n    images { id performers { id } __typename }"
        "\n    __typename"
        "\n  }"
        "\n}"
    )
    variables = {
        "filter": {"q": "", "page": 1, "per_page": PER_PAGE, "sort": "path", "direction": "ASC"},
        "image_filter": {"galleries": {"value": [gallery_id], "modifier": "INCLUDES_ALL"}},
        "image_ids": []
    }
    result = stash_graphql(query, variables)
    images = result.get("data", {}).get("findImages", {}).get("images", [])
    return [img["id"] for img in images if target_performer_id not in [p["id"] for p in img.get("performers", [])]]

# -------------------------------
# BULK UPDATE FUNCTIONS
# -------------------------------
def bulk_update_scenes(scene_ids, performer_id):
    query = (
        "mutation BulkSceneUpdate($input: BulkSceneUpdateInput!) {"
        "\n  bulkSceneUpdate(input: $input) { id __typename }"
        "\n}"
    )
    variables = {
        "input": {
            "ids": scene_ids,
            "performer_ids": {"mode": "ADD", "ids": [performer_id]},
            "tag_ids": {"mode": "ADD", "ids": []},
            "group_ids": {"mode": "ADD", "ids": []},
            "organized": False
        }
    }
    return stash_graphql(query, variables)

def bulk_update_images(image_ids, performer_id):
    query = (
        "mutation BulkImageUpdate($input: BulkImageUpdateInput!) {"
        "\n  bulkImageUpdate(input: $input) { id __typename }"
        "\n}"
    )
    variables = {
        "input": {
            "ids": image_ids,
            "performer_ids": {"mode": "SET", "ids": [performer_id]},
            "tag_ids": {"mode": "ADD", "ids": []},
            "gallery_ids": {"mode": "ADD", "ids": []},
            "organized": False
        }
    }
    return stash_graphql(query, variables)

def bulk_update_galleries(gallery_ids, performer_id):
    query = (
        "mutation BulkGalleryUpdate($input: BulkGalleryUpdateInput!) {"
        "\n  bulkGalleryUpdate(input: $input) { id __typename }"
        "\n}"
    )
    variables = {
        "input": {
            "ids": gallery_ids,
            "performer_ids": {"mode": "ADD", "ids": [performer_id]},
            "tag_ids": {"mode": "ADD", "ids": []},
            "organized": False
        }
    }
    return stash_graphql(query, variables)

# -------------------------------
# MAIN PROCESSING FUNCTION FOR A FOLDER
# -------------------------------
def process_folder(folder_path):
    try:
        folder_name = os.path.basename(folder_path)
        cleaned_name = clean_folder_name(folder_name)
        if not cleaned_name:
            log_issue(f"Skipping folder '{folder_path}' because cleaned name is empty.")
            return

        print(f"\nProcessing folder: {folder_path}")
        print(f"Searching for performer: '{cleaned_name}'")
        count, performers = find_performer(cleaned_name)
        if count == 0:
            log_issue(f"No local performer found for '{cleaned_name}' in folder '{folder_path}'. Attempting FansDB scrape.")
            new_perf_id = scrape_and_create_performer(cleaned_name)
            if new_perf_id:
                target_perf_id = new_perf_id
                aliases = []
            else:
                log_issue(f"Unable to scrape and create performer for '{cleaned_name}' in folder '{folder_path}'. Skipping folder.")
                return
        elif count > 1:
            log_issue(f"Multiple performers ({count}) found for '{cleaned_name}' in folder '{folder_path}'. Skipping folder.")
            return
        else:
            performer = performers[0]
            target_perf_id = performer["id"]
            aliases = performer.get("alias_list", [])
            print(f"  Found local performer '{performer['name']}' with ID {target_perf_id}")

        # Process Scenes
        scene_ids = find_scene_ids(folder_name, target_perf_id)
        print(f"  Found {len(scene_ids)} scenes in folder that need updating.")
        if scene_ids:
            scene_result = bulk_update_scenes(scene_ids, target_perf_id)
            updated_scene_count = len(scene_result.get("data", {}).get("bulkSceneUpdate", []))
            print(f"  Bulk updated {updated_scene_count} scenes.")
        else:
            print("  No scenes needed updating in this folder.")

        # Process Images & Galleries via Gallery lookup using folder name and aliases.
        search_terms = set([folder_name] + aliases)
        galleries_found = set()
        for term in search_terms:
            gallery_id = find_gallery_id_by_term(term)
            if gallery_id and gallery_id not in galleries_found:
                image_ids = find_images_from_gallery(gallery_id, target_perf_id)
                print(f"  For search term '{term}', found {len(image_ids)} images needing update in gallery {gallery_id}.")
                if image_ids:
                    image_result = bulk_update_images(image_ids, target_perf_id)
                    updated_count = len(image_result.get("data", {}).get("bulkImageUpdate", []))
                    print(f"  Bulk updated {updated_count} images in gallery {gallery_id}.")
                galleries_found.add(gallery_id)
            else:
                print(f"  Skipping search term '{term}' (no unique gallery found).")
        if galleries_found:
            gallery_result = bulk_update_galleries(list(galleries_found), target_perf_id)
            updated_gallery_count = len(gallery_result.get("data", {}).get("bulkGalleryUpdate", []))
            print(f"  Bulk updated {updated_gallery_count} galleries with performer.")
        else:
            print("  No galleries updated.")
    except Exception as e:
        log_issue(f"Error processing folder '{folder_path}': {e}")

# -------------------------------
# MAIN LOOP
# -------------------------------
def main():
    if not os.path.exists(BASE_FOLDER):
        print(f"Base folder '{BASE_FOLDER}' does not exist.")
        sys.exit(1)
    if len(sys.argv) > 1:
        folders = []
        for folder in sys.argv[1:]:
            full_path = os.path.join(BASE_FOLDER, folder)
            if os.path.isdir(full_path):
                folders.append(full_path)
            else:
                log_issue(f"Warning: Folder '{folder}' not found in base folder '{BASE_FOLDER}'.")
        if not folders:
            print("No valid folders provided. Exiting.")
            sys.exit(1)
    else:
        folders = [os.path.join(BASE_FOLDER, item) for item in os.listdir(BASE_FOLDER)
                   if os.path.isdir(os.path.join(BASE_FOLDER, item))]
    for folder_path in folders:
        process_folder(folder_path)

if __name__ == "__main__":
    main()
