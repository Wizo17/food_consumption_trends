

def get_ingredients_infos(ingredients, infos):
    # Todo Add description
    result = []
    for ingredient in ingredients:
        new_ig = {}
        for info in infos:
            if info in ingredient:
                new_ig[info] = ingredient[info]
            else:
                new_ig[info] = ""
        result.append(new_ig)
            
    return result

