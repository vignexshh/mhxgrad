import sys
import pandas as pd
import pymongo
from functools import lru_cache
import gradio as gr
import os
from dotenv import load_dotenv
import logging
from pymongo.errors import PyMongoError
from collections import OrderedDict
from typing import List, Any, Dict

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DATABASE_NAME = os.getenv("DATABASE_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
DEFAULT_EXCLUDED_FIELDS = {'_id', 'internal_metadata', 'audit_trail'}
if not MONGO_URI:
    logging.error("Environment variable MONGO_URI is not set.")
if not DATABASE_NAME:
    logging.error("Environment variable DATABASE_NAME is not set.")
if not COLLECTION_NAME:
    logging.error("Environment variable COLLECTION_NAME is not set.")


@lru_cache(maxsize=1)
def get_mongo_client(uri):
    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ismaster')
        return client
    except Exception as e:
        raise ConnectionError(f"MongoDB connection failed: {e}")


client = get_mongo_client(MONGO_URI)
if DATABASE_NAME is None:
    raise ValueError("DATABASE_NAME environment variable is not set.")
if COLLECTION_NAME is None:
    raise ValueError("COLLECTION_NAME environment variable is not set.")

db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

def get_distinct_values(target: str, criteria: Dict[str, Any] | None) -> List[Any]:
    distinct = collection.distinct(target, filter=criteria)
    return distinct

def update_subcategories(selected_category):
    subcategories = get_distinct_values("listSubCategory", criteria={"listCategory": selected_category})
    return gr.update(choices=subcategories)



def get_documents(category: Dict[str, Any], subcategory: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        query = {**category, **subcategory}
        documents = list(collection.find(query))
        return documents
    except PyMongoError as e:
        logging.error(f"MongoDB query failed: {e}")
    except Exception as e:
        logging.error(f"Unexpected error in get_documents: {e}")
    
    return []

def fetch_documents_ui(selected_category: str, selected_subcategory: str):
    return get_documents(
        {"listCategory": selected_category},
        {"listSubCategory": selected_subcategory}
    )

def create_interface():
    with gr.Blocks(css="footer{display:none !important}", title="MedicalHunt Neet Data Explorer 2024 less death") as demo:
        gr.Markdown("## MedicalHunt NEET Data Explorer 2024")
        with gr.Row(variant="compact"):
            category = gr.Dropdown(choices=get_distinct_values('listCategory', criteria=None), label="Select Category")
            sub_category = gr.Dropdown(choices=[], label="Select Sub-Category")
            output = gr.JSON(label="Matching Documents")
            
            

        category.change(
            fn=update_subcategories,
            inputs=category,
            outputs=sub_category
        )

        sub_category.change(
            fn=fetch_documents_ui,
            inputs=[category, sub_category],
            outputs=output

        )

    return demo

if __name__ == "__main__":
    app = create_interface()
    app.launch()