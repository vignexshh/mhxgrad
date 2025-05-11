import sys
import pandas as pd
import pymongo
from functools import lru_cache
import gradio as gr

# --- Configuration ---
MONGO_URI = ""
DATABASE_NAME = "MHneetData2024"
COLLECTION_NAME = "neetData2024"
DEFAULT_IGNORED_FIELDS = {'_id', 'internal_metadata', 'audit_trail'}

# --- MongoDB Helpers ---

@lru_cache(maxsize=1)
def get_mongo_client(uri):
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        return client
    except Exception as e:
        raise ConnectionError(f"MongoDB connection failed: {e}")

def get_database(client, db_name):
    return client[db_name]

def get_collection(db, collection_name):
    return db[collection_name]

def get_distinct_values(collection_name, field, query=None, timeout=600):

    client = get_mongo_client(MONGO_URI)
    db = get_database(client, DATABASE_NAME)
    collection = get_collection(db, collection_name)
    if query is None:
        query = {}
    try:
        return sorted([v for v in collection.distinct(field, query) if v is not None])
    except:
        return []

def get_available_fields(collection_name):
    client = get_mongo_client(MONGO_URI)
    db = get_database(client, DATABASE_NAME)
    collection = get_collection(db, collection_name)
    try:
        doc = collection.find_one()
        return set(doc.keys()) - {'_id', 'listCategory', 'listSubCategory'} if doc else set()
    except:
        return set()

def fetch_filtered_documents(collection_name, query, ignored_fields=None):
    client = get_mongo_client(MONGO_URI)
    db = get_database(client, DATABASE_NAME)
    collection = get_collection(db, collection_name)
    try:
        projection = {field: 0 for field in ignored_fields} if ignored_fields else None
        docs = list(collection.find(query, projection))
        df = pd.DataFrame(docs)
        if '_id' in df.columns:
            df.drop(columns=['_id'], inplace=True)
        return df
    except:
        return pd.DataFrame()

# --- Interface Callbacks ---

def update_subcategories(category):
    if not category:
        return gr.update(choices=[""], value="", interactive=False)
    subcategories = [""] + get_distinct_values(COLLECTION_NAME, 'listSubCategory', {'listCategory': category})
    return gr.update(choices=subcategories, value="", interactive=True)

def search_documents_simple(category, subcategory, ignored_fields_list):
    query = {}
    if category:
        query['listCategory'] = category
    if subcategory:
        query['listSubCategory'] = subcategory
    ignored_fields = set(ignored_fields_list) | DEFAULT_IGNORED_FIELDS
    df = fetch_filtered_documents(COLLECTION_NAME, query, ignored_fields)
    if df.empty:
        return pd.DataFrame(), "No documents found matching the selected criteria."
    return df, f"Found {len(df)} documents."

# --- Build Gradio UI ---

def create_interface():
    categories = [""] + get_distinct_values(COLLECTION_NAME, 'listCategory')
    available_fields = list(get_available_fields(COLLECTION_NAME))

    with gr.Blocks(css="footer{display:none !important}",title="MedicalHunt Neet Data Explorer 2024") as demo:
        gr.Markdown("## MedicalHunt NEET Data Explorer 2024")
        with gr.Row():
            category = gr.Dropdown(choices=categories, label="Select Category")
            subcategory = gr.Dropdown(choices=[""], label="Select SubCategory", interactive=False)
        ignored_fields = gr.CheckboxGroup(
            choices=sorted(available_fields),
            value=list(DEFAULT_IGNORED_FIELDS & set(available_fields)),
            label="Fields to ignore"
        )
        submit_btn = gr.Button("Search")
        output_df = gr.DataFrame()
        status_text = gr.Textbox(label="Status")

        category.change(fn=update_subcategories, inputs=category, outputs=subcategory)
        
        submit_btn.click(
            fn=search_documents_simple,
            inputs=[category, subcategory, ignored_fields],
            outputs=[output_df, status_text]
            
        )

    return demo

# --- Run ---
if __name__ == "__main__":
    app = create_interface()
    app.launch()
