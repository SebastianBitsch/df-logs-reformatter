import csv
import json
import re
import pandas as pd
from dateutil import parser
import argparse

def prompt_input_format():
    print("df_logs_reformatter.py -i <inputpath:csv> -o <outputpath:folder>")

# Get the paths to the inpit and output files from the arguments passed in the terminal
def get_files():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help="Path tonput file", required=True)
    parser.add_argument('-o', '--output', help="Output directory", required=True)
    args = parser.parse_args()

    return args.input, args.output


def iterate_textpayload_multidimensional(my_dict):
    return_dict = {
        "error_type": None,
        "session_id": None,
        "caller_id": None,
        "email": None,
        "code": None,
        "string_value": None,
        "lang": None,
        "speech": None, 
        "is_fallback_intent": None,
        "webhook_for_slot_filling_used": None,
        "webhook_used": None,
        "intent_name": None,
        "intent_id": None,
        "score": None,
        "action": None,
        "resolved_query": None,
        "source": None
    }

    for k,v in my_dict.items():
        if(isinstance(v,dict)):
            iterate_textpayload_multidimensional(v)
            continue
        if k in return_dict:
            return_dict[k] = v
    return return_dict

def iterate_multidimensional(my_dict):
    return_json = {
        "insertId": None,
        "logName": None,
        "receiveTimestamp": None,
        "textPayload": None,
        "timestamp": None,
        "trace": None
    }
    for k,v in my_dict.items():
        if(isinstance(v,dict)):
            iterate_multidimensional(v)
            continue
        if k in return_json:
            return_json[k] = v
    return return_json 

def iterate_textpayload(my_list):
    res = []
    for item in my_list:
        my_list_item = item.replace('"', '')
        if ':' in my_list_item:
            res.append(map(str.strip, my_list_item.split(":", 1)))
    return dict(res)


def parse_transform_response(pub_sub_data):
    fullpayload_dict = iterate_multidimensional(pub_sub_data)
    # Clean textPlayload from Stackdriver - not a valid JSON object
    text_payload = fullpayload_dict['textPayload']
    return_merged_payload = None

    if text_payload != None:
        regex = re.compile(r'''[\S]+:(?:\s(?!\S+:)\S+)+''', re.VERBOSE)
        matches = regex.findall(pub_sub_data["textPayload"])
        iterate_textpayload_response = iterate_textpayload(matches)
        textpayload_dict = iterate_textpayload_multidimensional(iterate_textpayload_response)
        if textpayload_dict["error_type"] is not None:
            textpayload_dict["error_type"] = textpayload_dict["error_type"].replace("\n", "").replace("}", "").strip()
        return_merged_payload = dict(list(fullpayload_dict.items()) + list(textpayload_dict.items()))
    if return_merged_payload is not None:
        return return_merged_payload
    else:
        return fullpayload_dict


def read_json(path) -> list[dict]:
    """
    Read a csv consisting of an array of json objects which are NOT seperated by commas,
    line by line and return a list with each line as a dict
    """
    input_blobs = []

    with open(path, encoding="utf-8") as f:
        for line in f:
            input_blobs.append(json.loads(line))
    return input_blobs
    
def export_to_csv(dataframe, path, index=False):
    dataframe.to_csv(path,sep=";", encoding="utf-8-sig", index=index)

def create_pivot_df(main_df) -> pd.DataFrame:
    pivot = main_df.intent_name.value_counts()
    pdf = pivot.to_frame()
    n_fallbacks = len(main_df.is_fallback_intent[main_df.is_fallback_intent == "true"])
    fallback_row = pd.DataFrame([n_fallbacks],index=["Default Fallback Intent"])
    pdf = pd.concat([pivot, fallback_row])
    pdf.columns = ["frequency"]
    
    return pdf

def create_history(df) -> list[list]:
    identifier = 'trace'
    conversation_ids = df[identifier].unique().tolist()
    conversations = []

    for id in conversation_ids:
        queries = df[df[identifier] == id]['resolved_query'].tolist()
        responses = df[df[identifier] == id]['string_value'].tolist()
        timestamp = df[df[identifier] == id]["timestamp"].astype(str).iloc[0]
        history = [timestamp]

        for q,a in zip(queries, responses):
            history.extend([q,a])
        conversations.append(history)
    return conversations


# Usage
# c:/Users/Sebastian/Documents/TestEnviornment/ReformatLogFiles/bq_reformatter.py -i C:\Users\Sebastian\Documents\TestEnviornment\ReformatLogFiles\input_data\logs_new.json -o C:\Users\Sebastian\Documents\TestEnviornment\ReformatLogFiles\ouput
if __name__ == "__main__":
    print("-----")
    print("Starting program")
    ifile, ofile = get_files()
    input_blobs = read_json(ifile)

    # Process lines in the file individually
    parsed_output = [parse_transform_response(i) for i in input_blobs]

    # Reformat using dataframe, and save to csv
    df = pd.DataFrame.from_dict(parsed_output, orient="columns")
    df = df.drop(columns=["textPayload","error_type","session_id","caller_id","email","action","speech"])
    df = df.drop(df[df.code != "200"].index)

    # Reformat timestamps and sort by them
    df['timestamp'] = df["timestamp"].apply(parser.parse)
    df['receiveTimestamp'] = df["receiveTimestamp"].apply(parser.parse)
    df = df.sort_values(by=["timestamp"])
    
    # Create and export pivot df
    pdf = create_pivot_df(df)
    export_to_csv(pdf, ofile + "/pivot.csv", index=True)
    
    # Create and export chat history df
    history = create_history(df)
    with open(ofile + "/history.csv", "w", encoding="utf-8-sig") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerows(history)

    # Export the raw data
    export_to_csv(df, ofile + "/raw.csv")

    print("Terminating program with succes: True")
    print("-----")