import re
import logging
from datetime import datetime
from conf import seller_type_config,excluded_words
from helpers import  count_months\
                    ,extract_numbers\
                    ,parse_date\
                    ,is_float\
                    ,get_key\
                    ,get_new_descriptions\
                    ,find_key_by_value\
                    ,remove_makes\
                    ,remove_descriptions\
                    ,custom_sort
                    
def clean_data(data,cleaning_functions,mapping_tables):
    try:
        logging.info("======== Starting the data cleaning process ========")
        for car in data:
            try:
                for key, value in car.items():
                    if key in cleaning_functions:
                        if key in ('doors','seats','gears'):
                            car[key] = cleaning_functions[key.lower()](value, key[:-1].upper())
                        elif key == 'model':
                            car[key] = cleaning_functions[key](value, car['make'],mapping_tables)
                        elif key == 'spec':
                            car[key] = cleaning_functions[key](value, car['make'],mapping_tables)
                        else:
                            car[key] = cleaning_functions[key.lower()](value)
            except Exception as e:
                logging.exception(e)
                continue
        logging.info("======== Data cleaning completed ========")
        return data
    except Exception as e:
        logging.exception("Error while cleaning data = ",e)


def trim_and_upper(input_string):
    try:
        # Check if input is string and not empty
        if input_string and isinstance(input_string,str):
            # Trim and convert to upper
            trimmed_and_upper = input_string.strip().upper()
        else:
            # Return input as is
            trimmed_and_upper = input_string

        # Return trimmed and upper case string
        return trimmed_and_upper
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {input_string}")
        return input_string


def cleaning_fuel_type(fuel_type):
    try:
        if fuel_type and isinstance(fuel_type,str):
            fuel_type = fuel_type.strip(" ")
            if fuel_type.lower() == 'petrol/lpg' or fuel_type.lower() == 'gasoline':
                fuel_type = 'PETROL'
            return fuel_type.strip().upper()
        return fuel_type
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {fuel_type}")
        return fuel_type


def clean_transmission(transmission):
    try:
        if transmission and isinstance(transmission,str):                      
            transmission = transmission.strip()    
            if transmission.upper() == 'A/T':   
                transmission = "AUTOMATIC"    
                return transmission
            return transmission.strip().upper()
        return transmission
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {transmission}")
        return transmission


def clean_engine_size(engine_size):
    try:
        if engine_size and isinstance(engine_size, str):
            engine_size = str(engine_size).strip()
            engine_size = re.sub(r'[^0-9.]', '', engine_size)

            if ( isinstance(float(engine_size),float) or isinstance(int(engine_size),int)) and float(engine_size)>0.0:
                engine_size = float(engine_size)
                
                if engine_size >= 700:
                    engine_size = engine_size/1000
                if '.' in str(engine_size):
                    if len(str(engine_size).split('.')[1]) > 2:
                        engine_size = str(round(engine_size, 1))+" L"
                    else:
                        engine_size = str(engine_size)+" L"
        else:
            if isinstance(engine_size,int) and float(engine_size)>0.0:
                engine_size = str(float(engine_size))+" L"
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {engine_size}")

    return engine_size


def clean_cylinders(cylinders):
    try:
        if cylinders and isinstance(cylinders, str):
            cylinders = cylinders.replace('Cyl', '').strip().upper() # Remove 'Cyl' and clean input
            return cylinders # Return cleaned input
        return cylinders
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {cylinders}")
        return cylinders


def cleaning_hp(hp):
    try:
        if hp:
            if isinstance(hp, (int, float)):
                value = "{:.0f}".format(hp)
                return f"{value.strip().upper()} HP"
            elif isinstance(hp, str):
                hp = hp.strip()
                if "/" in hp:
                    hp_chunks = hp.split('/')
                    for value in hp_chunks:
                        if 'HP' in value.upper():
                            return value.strip().upper()
                elif hp.isdigit():
                    return f"{hp.strip().upper()} HP"
                elif is_float(hp):
                    return f"{hp.strip().upper()} HP"
                elif hp=="":
                    return "N/A"
                else:
                    return hp
            else:
                return str(hp).strip().upper()
        return hp
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {hp}")
        return hp


def clean_by_type(value, type_str):
    try:
        # Check if value exists
        if value and isinstance(value,str) and '+' not in value:
            # Remove trailing spaces
            value = value.strip(" ")
            if value.isnumeric():
                if value == '1':
                    # Return value with type_str
                    return (value + ' ' + type_str).strip()
                # If value is not 1
                else:
                    # Return value with type_str and 'S'
                    return (value + ' ' + type_str + 'S').strip()
            else:
                # Remove digits from value
                value = ''.join(filter(str.isdigit, value))
                # If value is not empty
                if value:
                    # Recursively call function
                    return clean_by_type(value, type_str)
                # Return cleaned value in uppercase
                return value.strip().upper()
        else:
            if value == 1:
                return (str(value) + ' ' + type_str).strip()
            else:
                return (str(value) + ' ' + type_str + 'S').strip()
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {value}")
        return value


def clean_seller_type(seller_type):
    try:
        if seller_type:
        
            seller_type_config
            value = seller_type.strip().lower()

            if value in seller_type_config['independent_keys']:
                value = seller_type_config['independent_value']
            elif value in seller_type_config['franchise_keys']:
                value = seller_type_config['franchize_value']
            elif value in seller_type_config['large_independent_keys']:
                value = seller_type_config['large_independent_value']
            elif value in seller_type_config['owner_keys']:
                value = seller_type_config['owner_value']

            return value.strip()
        return seller_type
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {seller_type}")
        return seller_type


def clean_for_duration(line):
    try:
        if line and isinstance(line, str):
            line = line.strip().lower()

            if 'years' in line or 'year' in line:
                years = extract_numbers(line)
                return str(int(years)*12)+" MONTHS"
            if  'months' in line or 'month' in line:
                months = extract_numbers(line)
                return months+" MONTHS"
            if 'valid till' in line:
                year = re.sub(r'[^0-9-]', '', line)
                if year:
                    date = parse_date(year)
                    return count_months(date)+" MONTHS"
            if 'till' in line:
                pattern = r'(?:jan|feb|mar|apr|aay|jun|jul|aug|sep|oct|nov|dec)-\d{4}'
                matches = re.findall(pattern, line)
                date = parse_date(matches[0])
                return count_months(date)+" MONTHS"
            if 'until' in line:
                until_year = int(re.sub(r'[^0-9]', '', line))
                current_year = datetime.now().year
                current_month = datetime.now().month
                diff = ((until_year - current_year) * 12) + (12 - current_month)
                return str(diff)+" MONTHS"
            
            value_datetime = parse_date(line)
            
            if value_datetime:
                return count_months(value_datetime)+" MONTHS"

            warranty = line.split()
            if len(warranty) > 1 and parse_date(warranty[0]):
                current_date = datetime.now().date()
                warranty_date = parse_date(warranty[0])
                diff = ((warranty_date.year - current_date.year) * 12) + (warranty_date.month - current_date.month)
                return str(diff)+" MONTHS"
            
            return line
        return line
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {line}")
        return line


def clean_body_type(body_type):
    try:
        if body_type:
            misc = 'SALOON'
            string = body_type.strip().upper()
            pattern = r'\b(?:' + '|'.join(re.escape(word) for word in excluded_words) + r')\b'
            string = re.sub(pattern, '', string, flags=re.IGNORECASE).strip().upper()
            if string == misc:
                return 'SEDAN'

            matches = re.findall(r'\b(' + re.escape(misc) + r')\b', string, re.IGNORECASE)
            if matches:
                return 'SEDAN'
            return string
        return body_type
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {body_type}")
        return body_type

def cleaningModel(model,make,dataToMap):
    try:
        if model and make:
            model = model.strip().upper()
            mappingData = dataToMap['bb_model']
            
            models = list(map(str.upper, mappingData))
            
            if model in models:
                return model
                
            pieces = model.split(' ')
            if len(pieces)>2:
                key = get_key(pieces[0]+pieces[1]+" "+pieces[2],models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1]+pieces[2],models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+"-"+pieces[1]+" "+pieces[2],models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1]+"-"+pieces[2],models)
                if key:
                    return models[key]
            
            if len(pieces)>1:
                key = get_key(pieces[0]+pieces[1],models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1],models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+"-"+pieces[1],models)
                if key:
                    return models[key]

            # modelsString part is missing here
            NewModelDescriptions = get_new_descriptions(models)
            value = find_key_by_value(NewModelDescriptions,pieces[0])
            if value:
                return value
            value = find_key_by_value(NewModelDescriptions,pieces[0].replace("-", ""))
            if value:
                return value
            value = find_key_by_value(NewModelDescriptions,model.replace(' ', '').replace('-', ''))
            if value:
                return value
            
            possible_models = []
            for key, value in enumerate(models):
                regex = r'\b({})\b'.format(value)
                matches = re.findall(regex, model, re.IGNORECASE)
                if matches:
                    possible_models.append({'words': len(matches), 'value': matches[0]})
            
            if possible_models:
                if len(possible_models) == 1:
                    single_model = [item['value'] for item in possible_models]
                    return single_model[0]
                # model_array = sorted(possible_models, key=custom_sort, reverse=True)
                model_array = list(set(item['value'] for item in possible_models))
                for value in model_array:
                    regex = r'\b({})\b'.format(value)
                    matches = re.findall(regex, model, re.IGNORECASE)
                    if matches:
                        return matches[0].upper().strip()
                        
            makes = dataToMap['bb_make']
            makes = list(set(makes))
            model = remove_makes(model, makes)
            if model in models:
                return model

            fuel_types = dataToMap['bb_fuel']
            engine_sizes = dataToMap['bb_enginesize']
            body_types = dataToMap['bb_body']
            hps = dataToMap['bb_hp']
            specs = dataToMap['bb_specifications']

            engines = [size.replace(" ", "") for size in engine_sizes]
            array_to_search_and_remove = fuel_types+engine_sizes+body_types+engines+hps
            model = remove_descriptions(model, array_to_search_and_remove)
            if model in models:
                return model

            for value in specs:
                if not str(value).isnumeric():
                    matches = re.findall(r'\b({})\b'.format(re.escape(value)), model, re.IGNORECASE)
                    result = ''.join(matches)
                    if result:
                        if len(matches) > 1:
                            result = matches[0]
                        if len(result) != 1:
                            if result == model:
                                model = result
                            else:
                                model = model.replace(result, "").strip()
                            break
            
            if model.replace("  "," ") in models:
                return model
                
            if make.lower() == 'toyota':
                engine_number = [value.replace(" L", "") for value in engine_sizes]
                hp_number = [value.replace(" HP", "") for value in hps]
                search_and_remove = engine_number + hp_number
                string = model
                for term in search_and_remove:
                    string = string.replace(term, "")
                model = string.strip().upper().replace("  "," ")
                if model in models:
                    return model
            
            return model
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {model}")
        return model
    
    return model


def cleaningSpec(spec,make,dataToMap):
    try:
        if spec and make:
            spec = spec.strip().upper()
            mappingData = dataToMap['bb_specifications']
            specs = list(map(str.upper, mappingData))
            
            if spec in specs:
                return spec
                
            pieces = spec.split(' ')
            if len(pieces)>2:
                key = get_key(pieces[0]+pieces[1]+" "+pieces[2],specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1]+pieces[2],specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+"-"+pieces[1]+" "+pieces[2],specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1]+"-"+pieces[2],specs)
                if key:
                    return specs[key]
            
            if len(pieces)>1:
                key = get_key(pieces[0]+pieces[1],specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1],specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+"-"+pieces[1],specs)
                if key:
                    return specs[key]

            # specsString part is missing here
            NewspecDescriptions = get_new_descriptions(specs)
            value = find_key_by_value(NewspecDescriptions,pieces[0])
            if value:
                return value
            value = find_key_by_value(NewspecDescriptions,pieces[0].replace("-", ""))
            if value:
                return value
            value = find_key_by_value(NewspecDescriptions,spec.replace(' ', '').replace('-', ''))
            if value:
                return value
            
            possible_specs = []
            for key, value in enumerate(specs):
                regex = r'\b({})\b'.format(value)
                matches = re.findall(regex, spec, re.IGNORECASE)
                if matches:
                    possible_specs.append({'words': len(matches), 'value': matches[0]})
            
            if possible_specs:
                if len(possible_specs) == 1:
                    single_spec = [item['value'] for item in possible_specs]
                    return single_spec[0]
                spec_array = sorted(possible_specs, key=custom_sort, reverse=True)
                spec_array = list(set(item['value'] for item in possible_specs))
                for value in spec_array:
                    regex = r'\b({})\b'.format(value)
                    matches = re.findall(regex, spec, re.IGNORECASE)
                    if matches:
                        return matches[0].upper().strip()

            
            makes = makes = dataToMap['bb_make']
            makes = list(set(makes))
            spec = remove_makes(spec, makes)
            
            if spec in specs:
                return spec
                
            fuel_types = dataToMap['bb_fuel']
            engine_sizes = dataToMap['bb_enginesize']
            body_types = dataToMap['bb_body']
            hps = dataToMap['bb_hp']

            engines = [size.replace(" ", "").lower() for size in engine_sizes]
            
            array_to_search_and_remove = fuel_types+engine_sizes+body_types+engines+hps
            spec = remove_descriptions(spec, array_to_search_and_remove)
            if spec in specs:
                return spec
                
            if make.lower() == 'toyota':
                engine_number = [value.replace(" L", "") for value in engine_sizes]
                hp_number = [value.replace(" HP", "") for value in hps]
                search_and_remove = engine_number + hp_number
                string = spec
                for term in search_and_remove:
                    string = string.replace(term, "")
                spec = string.strip().upper().replace("  "," ")
                if spec in specs:
                    return spec
            
            return spec
    except Exception as e:
        logging.exception(f"Error: {e} === Value: {spec}")
        return spec
    
    return spec
