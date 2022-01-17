from os import remove
import gcsfs
import json
import cloudstorage as gcs
import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import re
from nlp_apply import nlp
from check_apply import check
from maps_apply import geocode, Country
import datetime as datetime

PROJECT='YOUR PROJECT'
BUCKET='YOUR BUCKET'

### ------ ###

## DOB check ##
def valid_date(date_string):
    mat = re.match(r"(\d{4})[/.-](\d[0-9])[/.-](\d[0])$", date_string)
    if mat:
        new_substr = mat.group(1) + "/06/30"
        return new_substr
    else:
        return date_string

def derived_date(date_string):
    mat = re.match(r"(\d{4})[/.-](\d[0-9])[/.-](\d[0])$", date_string)
    if mat:
        new_substr = "Derived_Date"
        return new_substr
    else:
        new_substr = "Original_Date"
        return new_substr

def norm_date(datestring1, datestring2):
    mat1=re.match(r"(\d{4})[/.-](\d[0-9])[/.-](\d[0-9])$", datestring1)
    mat2=re.match(r"(\d{4})[/.-](\d[0-9])[/.-](\d[0-9])$", datestring2)

    if mat1 and mat2:
        new_substr = re.sub(r"[0-9]{4}", mat2.groups(1), datestring1)
        return new_substr
    else:
        return datestring1

def different_date(dob1, dob2):
    delta = dob2 - dob1
    return abs(delta.days)

def dob_decision(dob1, dob2, dob3, dob4):
    if pd.isnull(dob1) or pd.isnull(dob2):
        return 1
    elif dob3 < 510 or dob4 < 30:
        return 1
    else:
        return 0

## Apply NLP ##
# Search on name that alert is raised on

def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return s

def apply_find_between(df, start, end):
    name_on_alert = []
    start = '<cm>'
    end = '</cm>'
    for name in list(df['INPUT']):
        name_on_alert.append(find_between(name, start, end))
    df['input_name_transformed'] = name_on_alert
    return df


def is_entity_match(x):
    while x['entity_type_input'] != 'OTHER' and x['entity_type_match'] != 'OTHER':
        if x['entity_type_input'] == x['entity_type_match']:
            return 1
        else: 
            return 0 
    else:
        return 1 

def all_conditions(x):
    check_all = check(x)
    yield check_all.entity()
    yield check_all.DOB()


def check_all_conditions(x):
    for condition in all_conditions(x):
        if condition:
            return condition
    return None


def alert_decision(x):
    if x['is_score'] == 0:
        return 'Potentially request release'
    else:
        return 'Further check'


def apply_function_nlp_all(df_1):
    df_1 = apply_find_between(df_1, '<cm>', '</cm>')
    selected_column = ['ALERT_IDENTIFIER',
                 'Field_Name',
                 'INPUT',
                 'Match_Data',
                 'input_name_transformed',
                 'is_DOB']
    df_1 = df_1[selected_column]
    df_1 = df_1.sample(frac=1)
    df_1['entity_type_input'] = df_1['input_type_transformed'].apply(lambda x: nlp(x).create_entity_gcp())
    df_1['entity_type_match'] = df_1['Match_Data'].apply(lambda x: nlp(x).create_entity_gcp())
    df_1['is_entity'] = df_1.apply(is_entity_match, axis=1)
    df_1['is_match'] = df_1.apply(check_all_conditions, axis=1)
    return df_1


### Address check
def remove_string(text):
    string_to_remove = ["<cm>", "</cm>"]
    new_string = text
    for string in string_to_remove:
        new_string = new_string.replace(string, "")
    return new_string


def apply_get_country(x):
    x["loc_input_country"] = x["loc_input"].apply(lambda x : Country(x).get_country())
    x["loc_wc_country"] = x["LOCATION_COUNTRY"]
    fill_missing_wc_location = x["loc_wc"].apply(lambda x: Country(x).get_country())
    x["loc_wc_country"].fillna(fill_missing_wc_location, inplace=True)
    return x 


def is_address_match(x):
    while x['loc_input_country'] is not None and x["loc_wc_country"] is not None:
        if x['loc_input_country'] == x["loc_wc_country"]:
            return 1 
        else: 
            return 0 
    else:
        return 1 

def apply_address_all(df_2):
    df_2["address"] = df_2["INPUT"].apply(remove_string)
    df_2["loc_input"] = df_2['address'].apply(lambda x: geocode(x).get_geocode())
    df_2["loc_wc"] = df_2['Match_Data'].apply(lambda x: geocode(x).get_geocode())
    df_2 = apply_get_country(df_2)
    df_2['is_address'] = df_2.apply(is_address_match, axis=1)
    df_2['is_match'] = df_2.apply(lambda x: check(x).address(), axis=1)
    return df_2


def combine_all_check(df_1, df_2):
    df_1 = apply_function_nlp_all(df_1)
    df_2 = apply_address_all(df_2)
    final_columns = ['ALERT_IDENTIFIER', 'Field_Name', 'Match_Data', 'INPUT', 'is_match']
    df_1 = df_1[final_columns]
    df_2 = df_2[final_columns]
    new_df = df_1.append(df_2, ignore_index=True)
    weight_0 = ['FP - DOB not a match', 'FP - Full name not a match', 'FP - Address not a match']
    new_df['is_score'] = np.where(new_df['is_match'].isin(weight_0), 0, 1)
    alert_df = new_df.groupby(['ALERT_IDENTIFIER'])['is_score'].sum().reset_index()
    alert_df['is_alert'] = alert_df.apply(alert_decision, axis=1)
    return alert_df



### ------ ###

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_name = event['name']
    file_path = 'gs://{0}/{1}'.format(BUCKET,file_name)
    gcs_file_system = gcsfs.GCSFileSystem(project=PROJECT)
    with gcs_file_system.open(file_path) as f:
        df = pd.read_csv(f)
    print(f"file name: {file_name}.")
    print(f"data: {df}")
    df = df.where(pd.notnull(df), None)
    df_2 = df[df['Field_Name'].str.contains("_address")]
    df_2 = apply_address_all(df_2)

    print(f"is address match? : {df_2['is_match']}")
    print(f"loc_input_country : {df_2['loc_input_country']}")
    print(f"loc_wc_country : {df_2['loc_wc_country']}")






    
