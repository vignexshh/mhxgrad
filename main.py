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

# def fetch_documents_ui(selected_category: str, selected_subcategory: str):
#     return get_documents(
#         {"listCategory": selected_category},
#         {"listSubCategory": selected_subcategory}
#     )


def get_document_fields(category:str, subcategory:str) -> Dict[str, List[Any]]:
    documents = get_documents({"listCategory": category}, {"listSubCategory":subcategory})
    if not documents:
        return {}
    
    field_values: Dict[str, set] = {}
    for doc in documents:
        for key, value in doc.items():
            if key == "_id":
                continue
            field_values.setdefault(key, set()).add(value)
    return {key: sorted(values) for key, values in field_values.items()} #use this returns to build dynamic dropdowns from keys and choices from it's values 



def create_interface():
    with gr.Blocks(css="footer{display:none !important}", title="MedicalHunt Neet Data Explorer 2024 less death") as demo:
        gr.Markdown("## MedicalHunt NEET Data Explorer 2024")
        with gr.Row(variant="compact"):
            category = gr.Dropdown(choices=get_distinct_values('listCategory', criteria=None), label="Select Category")
            sub_category = gr.Dropdown(choices=[], label="Select Sub-Category")
            output_distinct_key_value_pairs = gr.JSON(label="Matching Documents") #remove later
            distinct_key_value_pairs = gr.State()
            
            

        category.change(
            fn=update_subcategories,
            inputs=category,
            outputs=sub_category
        )

        # sub_category.change(
        #     fn=fetch_documents_ui,
        #     inputs=[category, sub_category],
        #     outputs=output

        # )

        sub_category.change(
            fn=get_document_fields,
            inputs=[category, sub_category],
            outputs=[output_distinct_key_value_pairs, distinct_key_value_pairs]
        )

    return demo

if __name__ == "__main__":
    app = create_interface()
    app.launch()


# function.change(
#     fn=update_subcategories,         # function to run when value changes
#     inputs=category,                 # input to the function: current value of `category`
#     outputs=sub_category             # output from the function updates the `sub_category` dropdown
# )
