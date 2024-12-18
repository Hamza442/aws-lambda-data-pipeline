fields_data = {
    'job_id': 'job_id',
    'spider': 'spider',
    'Car_Name': 'car_name',
    'Car_URL': 'car_url',
    'City': 'city',
    'Country': 'country',
    'Doors': 'doors',
    'Last_Code_Update_Date': 'last_code_update_date',
    'Make': 'make',
    'Price_Currency': 'price_currency',
    'Scrapping_Date': 'scrapping_date',
    'Seller_Name': 'seller_name',
    'Seller_Type': 'seller_type',
    'Source': 'source',
    'Spec': 'spec',
    'Vehicle_Type': 'vehicle_type',
    'Year': 'year',
    'Acceleration': 'acceleration',
    'asking_price_ex_VAT': 'asking_price_ex_vat',
    'asking_price_inc_VAT': 'asking_price_inc_vat',
    'autodata_Make': 'ad_make',
    'autodata_Make_id': 'ad_make_id',
    'autodata_Spec': 'ad_spec',
    'autodata_Spec_id': 'ad_spec_id',
    'autodata_bodystyle': 'ad_body_type',
    'autodata_bodystyle_id': 'ad_body_type_id',
    'autodata_model': 'ad_model',
    'autodata_model_id': 'ad_model_id',
    'autodata_transmission': 'ad_transmission',
    'autodata_transmission_id': 'ad_transmission_id',
    'bodystyle': 'body_type',
    'colour_exterior': 'colour_exterior',
    'colour_interior': 'colour_interior',
    'condition': 'condition',
    'cylinders': 'no_of_cylinders',
    'engine_size': 'engine_size',
    'engine_unit': 'engine_unit',
    'fuel_type': 'fuel_type',
    'gearbox': 'gears',
    'horse_power': 'hp',
    'meta': 'meta',
    'mileage': 'mileage',
    'mileage_unit': 'mileage_unit',
    'model': 'model',
    'pdf': 'pdf',
    'seats': 'seats',
    'service_contract': 'service_contract',
    'service_contract_untill_when': 'service_contract_untill_when',
    'top_speed_kph': 'top_speed_kph',
    'torque_Nm': 'torque_nm',
    'transmission': 'transmission',
    'trim': 'trim',
    'vat': 'vat',
    'vin': 'vin',
    'warranty': 'warranty',
    'warranty_untill_when': 'warranty_untill_when',
    'wheel_size': 'wheel_size',
    'adjusted_price': 'adjusted_price',
    'regional_spec': 'regional_spec',
    'drive_type': 'drive_type',
    'Extras': 'extras',
    'Options': 'options',
    'img': 'img',
    'offers': 'offers',
}

seller_type_config = {
    'franchise_keys':
        ['official dealer', 'official dealers', 'dealership/certified pre-owned', 'dealership', 'certified pre-owned'],
    'independent_keys': ['independent dealer', 'dealer', 'independent dealers'],
    'large_independent_keys': ['large independent dealer', 'large independent dealers'],
    'owner_keys': ['owner'],
    'franchize_value': 'Franchise Dealer',
    'independent_value': 'Independent Dealer',
    'large_independent_value': 'Large Independent Dealer',
    'owner_value': 'Owner'
}

excluded_words = ['4Wd', '2WD', 'AWD', '2X2', '4X4', '4X2', 'FWD']

cols_to_map=['year','doors', 'body_type', 'transmission', 'no_of_cylinders', 'fuel_type', 'gears', 'seats', 'colour_in','make', 'model', 'spec']

cols_to_mapping_tbl = {
                  'year':'bb_modelyear'
                  ,'doors':'bb_doors'
                  ,'seats':'bb_seats'
                  ,'gears':'bb_gears'
                  ,'no_of_cylinders':'bb_noofcyls'
                  ,'fuel_type':'bb_fuel'
                  ,'body_type':'bb_body'
                  ,'engine_size':'bb_enginesize'
                  ,'transmission':'bb_transmissions'
                  ,'make':'bb_make'
                  ,'model': 'bb_model'
                  ,'spec': 'bb_specifications'
                }

rename_mastercode_cache = {'model_year': 'year_id', 'make': 'make_id', 'model': 'model_id', 'doors': 'doors_id', 
               'body_type': 'body_type_id', 'transmission': 'transmission_id', 'no_of_cyls': 'no_of_cylinders_id', 
               'fuel': 'fuel_type_id', 'gears': 'gears_id', 'seats': 'seats_id', 'spec': 'spec_id'}

COMPOSITE_KEY = ['year_id', 'make_id', 'model_id', 'doors_id', 'body_type_id', 'transmission_id', 'no_of_cylinders_id', 'fuel_type_id', 'gears_id', 'seats_id','spec_id']
