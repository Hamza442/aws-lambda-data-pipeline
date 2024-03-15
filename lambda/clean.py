import re
from datetime import datetime
from conf import seller_type_config,excluded_words
from helpers import  count_months\
                    ,extract_numbers\
                    ,parse_date\
                    ,is_float
                    
def clean_data(data,cleaning_functions):
    try:
        print("======== Starting the data cleaning process ========")
        for car in data:
            for key, value in car.items():
                if key in cleaning_functions:
                    if key in ('doors','seats','gears'):
                        car[key] = cleaning_functions[key.lower()](value, key[:-1].upper())
                    else:
                        # print(key)
                        car[key] = cleaning_functions[key.lower()](value)
        print("======== Data cleaning completed ========")
        return data
    except Exception as e:
        print("Error while cleaning data = ",e)


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
        print(f"Error: {e} === Value: {input_string}")
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
        print(f"Error: {e} === Value: {fuel_type}")
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
        print(f"Error: {e} === Value: {transmission}")
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
        print(f"Error: {e} === Value: {engine_size}")

    return engine_size


def clean_cylinders(cylinders):
    try:
        if cylinders and isinstance(cylinders, str):
            cylinders = cylinders.replace('Cyl', '').strip().upper() # Remove 'Cyl' and clean input
            return cylinders # Return cleaned input
        return cylinders
    except Exception as e:
        print(f"Error: {e} === Value: {cylinders}")
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
        print(f"Error: {e} === Value: {hp}")
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
        print(f"Error: {e} === Value: {value}")
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
        print(f"Error: {e} === Value: {seller_type}")
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
        print(f"Error: {e} === Value: {line}")
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
        print(f"Error: {e} === Value: {body_type}")
        return body_type
