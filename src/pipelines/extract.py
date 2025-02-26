from openfoodfacts import ProductDataset
from common.utils import dict_to_string, log_message
from pipelines.raw_data_infos import DATA_KEYS_OFF, INGREDIENTS_INFOS_KEYS, RAW_DATA_ARRAY_FIELDS
from pipelines.transform import get_ingredients_infos


def get_open_food_fact_dataset(size = 0):
    # TODO Add documentation

    if size < 0:
        return []
    
    dataset = ProductDataset()
    product_list = []
    
    try:
        for i, product in enumerate(dataset):
            if i == size:
                break

            item = {}
            for key in DATA_KEYS_OFF:
                if key in product:
                    # Particular keys
                    if key == "ingredients":
                        item[key] = get_ingredients_infos(product[key], INGREDIENTS_INFOS_KEYS)
                    elif key == "nutriments":
                        item[key] = dict_to_string(product[key])
                    else:
                        item[key] = product[key]
                else:
                    # TODO Program for array and dict type
                    # item[key] = None
                    if key in RAW_DATA_ARRAY_FIELDS:
                        item[key] = []
                    else:
                        item[key] = None

            product_list.append(item)

        return product_list
    except Exception as e:
        log_message("ERROR", f"An error occurred during data retrieval: {str(e)}")
        return []
        
