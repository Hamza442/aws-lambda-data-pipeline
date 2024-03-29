import json
import re
from datetime import datetime
from conf import seller_type_config, excluded_words, fields_data
from helpers import count_months, extract_numbers, parse_date, is_float, get_key, get_new_descriptions,\
    find_key_by_value, remove_makes, remove_descriptions, custom_sort


def rename_columns_and_clean_data(contents, cleaning_functions, mapping_tables, logger) -> list[dict[str, str]]:
    """
    Apply cleaning functions depending upon attribute name

    Args:
        contents: files contents read from s3
        cleaning_functions : dictionary of column name as key and function name as value
        mapping_tables : list of dictionaries of backbone mapping tables
        logger: logger
    """
    logger.info("======== Starting the data cleaning process ========")
    data = []
    for item in contents:
        try:
            raw_car = json.loads(item.decode('utf-8'))
            car = {}
            for raw_key, value in raw_car.items():
                key = fields_data.get(raw_key, raw_key)
                if key in cleaning_functions:
                    if key in ('doors', 'seats', 'gears'):
                        car[key] = cleaning_functions[key](value, key[:-1].upper(), logger)
                    elif key == 'model':
                        car[key] = cleaning_functions[key](value, raw_car['Make'], mapping_tables, logger)
                    elif key == 'spec':
                        car[key] = cleaning_functions[key](value, raw_car['Make'], mapping_tables, logger)
                    elif key == 'body_type':
                        car[key] = cleaning_functions[key](value, car['spec'], car['model'], mapping_tables , excluded_words)
                    else:
                        car[key] = cleaning_functions[key](value, logger)
                else:
                    car[key] = value

            seller_name, url, vin = car.get('seller_name', ''), car['meta'].get('url', ''), car.get('vin', '')
            car['tracking_id'] = re.sub(r'[^A-Za-z0-9]', '', seller_name + url if not vin else seller_name + vin)    
                
            data.append(car)
        except Exception as e:
            logger.exception(e)
            continue
    logger.info("======== Data cleaning completed ========")
    return data


def trim_and_upper(input_string, logger):
    """
    Trim and convert to upper case

    Args:
        input_string : column  value from car data
        logger: logger
    """
    try:
        # Check if input is string and not empty
        if input_string and isinstance(input_string, str):
            # Trim and convert to upper
            trimmed_and_upper = input_string.strip().upper()
        else:
            # Return input as is
            trimmed_and_upper = input_string

        # Return trimmed and upper case string
        return trimmed_and_upper
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {input_string}")
        return input_string


def cleaning_fuel_type(fuel_type, logger):
    """
    Clean  Fuel Type field by replacing 
    values with correct form of the fuel type

    Args:
        fuel_type : fuel type from car data
        logger: logger
    """
    try:
        if fuel_type and isinstance(fuel_type, str):
            fuel_type = fuel_type.strip(" ")
            if fuel_type.lower() == 'petrol/lpg' or fuel_type.lower() == 'gasoline':
                fuel_type = 'PETROL'
            return fuel_type.strip().upper()
        return fuel_type
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {fuel_type}")
        return fuel_type


def clean_transmission(transmission, logger):
    """
    Clean  Transmission Field by replacing  
    values with correct form of the transmission
    
    Args:
        transmission : transmission type from car data
        logger: logger
    """
    try:
        if transmission and isinstance(transmission, str):
            transmission = transmission.strip()    
            if transmission.upper() == 'A/T':   
                transmission = "AUTOMATIC"    
                return transmission
            return transmission.strip().upper()
        return transmission
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {transmission}")
        return transmission


def clean_engine_size(engine_size, logger):
    """
    Clean Engine Size filed by adding 
    liters to end of  numeric value

    Args:
        engine_size : engine size of the car in liters
        logger: logger
    """
    try:
        if engine_size and isinstance(engine_size, str):
            engine_size = str(engine_size).strip()
            engine_size = re.sub(r'[^0-9.]', '', engine_size)

            if (isinstance(float(engine_size), float) or isinstance(int(engine_size), int)) and \
                    float(engine_size) > 0.0:
                engine_size = float(engine_size)
                
                if engine_size >= 700:
                    engine_size = engine_size/1000
                if '.' in str(engine_size):
                    if len(str(engine_size).split('.')[1]) > 2:
                        engine_size = str(round(engine_size, 1))+" L"
                    else:
                        engine_size = str(engine_size)+" L"
        else:
            if isinstance(engine_size, int) and float(engine_size) > 0.0:
                engine_size = str(float(engine_size))+" L"
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {engine_size}")

    return engine_size


def clean_cylinders(cylinders, logger):
    """
       Clean cylinders column value by
       replace string  with empty string

    Args:
        cylinders : number of cylinders car engine have
        logger: logger
    """
    try:
        if cylinders and isinstance(cylinders, str):
            cylinders = cylinders.replace('Cyl', '').strip().upper()  # Remove 'Cyl' and clean input
            return cylinders  # Return cleaned input
        return cylinders
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {cylinders}")
        return cylinders


def cleaning_hp(hp, logger):
    """
    Clean horse power column of car data
    by removing unnecessary characters and
    adding HP at the end of number or string

    Args:
        hp : horse power of the car
        logger: logger
    """
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
                elif hp == "":
                    return "N/A"
                else:
                    return hp
            else:
                return str(hp).strip().upper()
        return hp
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {hp}")
        return hp


def clean_by_type(value, type_str, logger):
    """
    Clean  a specific field based on its type
    type can be doors,seats,gears

    Args:
        value : different type of car parts
        type_str : car parts such as door,seat,gear
        logger: logger
    """
    try:
        # Check if value exists
        if value and isinstance(value, str) and '+' not in value:
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
                    return clean_by_type(value, type_str, logger)
                # Return cleaned value in uppercase
                return value.strip().upper()
        else:
            if value == 1:
                return (str(value) + ' ' + type_str).strip()
            else:
                return (str(value) + ' ' + type_str + 'S').strip()
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {value}")
        return value


def clean_seller_type(seller_type, logger):
    """
    Clean  seller type by replace values
    from a seller type dict in config
    Args:
        seller_type : car seller type
        logger: logger
    """
    try:
        if seller_type:
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
        logger.exception(f"Error: {e} === Value: {seller_type}")
        return seller_type


def clean_for_duration(line, logger):
    """Find out car warranty duration in months

    Args:
        line : dates of car warranty
        logger: logger
    """
    try:
        if line and isinstance(line, str):
            line = line.strip().lower()

            if 'years' in line or 'year' in line:
                years = extract_numbers(line)
                return str(int(years)*12)+" MONTHS"
            if 'months' in line or 'month' in line:
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
        logger.exception(f"Error: {e} === Value: {line}")
        return line


def clean_body_type(body_type, spec, model, dataToMap , excluded_words):

    misc = 'SALOON'
    bodystyle = body_type
    bodyTypes = dataToMap['bb_body']
    excludedWords = excluded_words
    flag = True if body_type else False

    output = []

    if flag:
        # Find the index of the string in the uppercase body types list
        key = next((i for i, item in enumerate(bodyTypes) if item.upper() == bodystyle), None)

        if key is not None:
            output.append(bodyTypes[key])
        else:
            # Remove excluded words and trim
            for excluded_word in excludedWords:
                bodystyle = bodystyle.replace(excluded_word.upper(), "")
            bodystyle = bodystyle.strip()

            # Check again for the cleaned string
            key = next((i for i, item in enumerate(bodyTypes) if item.upper() == bodystyle), None)

            if key is not None:
                output.append(bodyTypes[key])
            elif bodystyle == misc:
                output.append('SEDAN')
            else:
                for value in bodyTypes:
                    matches = re.findall(r'\b(' + re.escape(value) + r')\b', bodystyle, re.IGNORECASE)
                    if matches:
                        output.append(matches[0].strip().upper())
                        break
                else:
                    matches = re.findall(r'\b(' + re.escape(misc) + r')\b', bodystyle, re.IGNORECASE)
                    if matches:
                        output.append(matches[0].strip().upper())
                    else:
                        output.append(None)
    else:
        if spec is not None:
            # Remove excluded words and trim for spec
            for excluded_word in excludedWords:
                spec = spec.replace(excluded_word.upper(), "")
            spec = spec.strip()

            # Search for matches in spec
            spec_match = False
            for value in bodyTypes:
                matches = re.findall(r'\b(' + re.escape(value) + r')\b', spec, re.IGNORECASE)
                if matches:
                    output.append(matches[0].strip().upper())
                    spec_match = True
                    break

            if not spec_match:
                matches = re.findall(r'\b(' + re.escape(misc) + r')\b', spec, re.IGNORECASE)
                if matches:
                    output.append(matches[0].strip().upper())
                else:
                    output.append(None)
        else:
            if model is not None:
                # Remove excluded words and trim for model
                for excluded_word in excludedWords:
                    model = model.replace(excluded_word.upper(), "")
                model = model.strip()

                # Search for matches in model
                model_match = False
                for value in bodyTypes:
                    matches = re.findall(r'\b(' + re.escape(value) + r')\b', model, re.IGNORECASE)
                    if matches:
                        output.append(matches[0].strip().upper())
                        model_match = True
                        break

                if not model_match:
                    matches = re.findall(r'\b(' + re.escape(misc) + r')\b', model, re.IGNORECASE)
                    if matches:
                        output.append(matches[0].strip().upper())
                    else:
                        output.append(None)
            else:
                output.append(None)
    return output[0]


def cleaning_model(model, make, data_to_map, logger):
    """
    Clean the model of the car by matching
    value in the backbone tables for mapping

    Args:
        model : model of the car
        make : make of the car
        data_to_map : mapping tables in backbone db
        logger: logger
    """
    try:
        if model and make:
            model = model.strip().upper()
            mapping_data = data_to_map['bb_model']
            
            models = list(map(str.upper, mapping_data))
            
            if model in models:
                return model
                
            pieces = model.split(' ')
            if len(pieces) > 2:
                key = get_key(pieces[0]+pieces[1]+" "+pieces[2], models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1]+pieces[2], models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+"-"+pieces[1]+" "+pieces[2], models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1]+"-"+pieces[2], models)
                if key:
                    return models[key]
            
            if len(pieces) > 1:
                key = get_key(pieces[0]+pieces[1], models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+" "+pieces[1], models)
                if key:
                    return models[key]
                key = get_key(pieces[0]+"-"+pieces[1], models)
                if key:
                    return models[key]

            # modelsString part is missing here
            new_model_descriptions = get_new_descriptions(models)
            value = find_key_by_value(new_model_descriptions, pieces[0])
            if value:
                return value
            value = find_key_by_value(new_model_descriptions, pieces[0].replace("-", ""))
            if value:
                return value
            value = find_key_by_value(new_model_descriptions, model.replace(' ', '').replace('-', ''))
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
                        
            makes = data_to_map['bb_make']
            makes = list(set(makes))
            model = remove_makes(model, makes)
            if model in models:
                return model

            fuel_types = data_to_map['bb_fuel']
            engine_sizes = data_to_map['bb_enginesize']
            body_types = data_to_map['bb_body']
            hps = data_to_map['bb_hp']
            specs = data_to_map['bb_specifications']

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
            
            if model.replace("  ", " ") in models:
                return model
                
            if make.lower() == 'toyota':
                engine_number = [value.replace(" L", "") for value in engine_sizes]
                hp_number = [value.replace(" HP", "") for value in hps]
                search_and_remove = engine_number + hp_number
                string = model
                for term in search_and_remove:
                    string = string.replace(term, "")
                model = string.strip().upper().replace("  ", " ")
                if model in models:
                    return model
            
            return model
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {model}")
        return model
    
    return model


def cleaning_spec(spec, make, data_to_map, logger):
    """
    Clean specification of car by matching
    it with different mapping tables in the
    backbone database

    Args:
        spec : specification of the car
        make : make of the car
        data_to_map : mapping tables in backbone db
        logger: logger
    """
    try:
        if spec and make:
            spec = spec.strip().upper()
            mapping_data = data_to_map['bb_specifications']
            specs = list(map(str.upper, mapping_data))
            
            if spec in specs:
                return spec
                
            pieces = spec.split(' ')
            if len(pieces) > 2:
                key = get_key(pieces[0]+pieces[1]+" "+pieces[2], specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1]+pieces[2], specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+"-"+pieces[1]+" "+pieces[2], specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1]+"-"+pieces[2], specs)
                if key:
                    return specs[key]
            
            if len(pieces) > 1:
                key = get_key(pieces[0]+pieces[1], specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+" "+pieces[1], specs)
                if key:
                    return specs[key]
                key = get_key(pieces[0]+"-"+pieces[1], specs)
                if key:
                    return specs[key]

            # specsString part is missing here
            new_spec_descriptions = get_new_descriptions(specs)
            value = find_key_by_value(new_spec_descriptions, pieces[0])
            if value:
                return value
            value = find_key_by_value(new_spec_descriptions, pieces[0].replace("-", ""))
            if value:
                return value
            value = find_key_by_value(new_spec_descriptions, spec.replace(' ', '').replace('-', ''))
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
                spec_array = list(set(item['value'] for item in spec_array))
                for value in spec_array:
                    regex = r'\b({})\b'.format(value)
                    matches = re.findall(regex, spec, re.IGNORECASE)
                    if matches:
                        return matches[0].upper().strip()

            makes = data_to_map['bb_make']
            makes = list(set(makes))
            spec = remove_makes(spec, makes)
            
            if spec in specs:
                return spec
                
            fuel_types = data_to_map['bb_fuel']
            engine_sizes = data_to_map['bb_enginesize']
            body_types = data_to_map['bb_body']
            hps = data_to_map['bb_hp']

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
                spec = string.strip().upper().replace("  ", " ")
                if spec in specs:
                    return spec
            
            return spec
    except Exception as e:
        logger.exception(f"Error: {e} === Value: {spec}")
        return spec
    
    return spec
