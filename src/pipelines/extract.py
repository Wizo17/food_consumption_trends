from openfoodfacts import ProductDataset
from common.utils import dict_to_string, log_message
from pipelines.raw_data_infos import DATA_KEYS_OFF, INGREDIENTS_INFOS_KEYS, RAW_DATA_ARRAY_FIELDS
from pipelines.transform import get_ingredients_infos
from common.config import global_conf


def get_open_food_fact_dataset(size = 0):
    # TODO Add documentation

    if size < 0:
        return []
    if global_conf.get('GENERAL.ENV') == "dev" and (size == 0 or size > int(global_conf.get('DEV.DEV_LIMIT_SIZE_DATA'))):
        size = int(global_conf.get('DEV.DEV_LIMIT_SIZE_DATA'))
    
    dataset = ProductDataset()
    product_list = []
    
    try:
        for i, product in enumerate(dataset):
            if i == size and size != 0:
                break

            item = {}
            for key in DATA_KEYS_OFF:
                if key in product:
                    # Particular keys
                    if key == "ingredients":
                        item[key] = str(get_ingredients_infos(product[key], INGREDIENTS_INFOS_KEYS))
                    elif key == "nutriments":
                        item[key] = str(product[key]) # dict_to_string(product[key])
                    elif key == "ecoscore_score":
                        item[key] = float(product[key])
                    else:
                        if isinstance(product[key], str):
                            item[key] = product[key].replace('"', " ").replace("'", " ").replace("(", " ").replace(")", " ")
                        else:
                            item[key] = product[key]
                else:
                    if key in RAW_DATA_ARRAY_FIELDS:
                        item[key] = []
                    elif key == "ecoscore_score":
                        item[key] = -1.0
                    else:
                        item[key] = ""

            product_list.append(item)

        return product_list
    except Exception as e:
        log_message("ERROR", f"An error occurred during data retrieval: {str(e)}")
        return []
        
