from datetime import datetime
import os
import json

today = datetime.now().strftime("%d-%m-%Y")
log_file = f"logs/log-{today}.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)

DEFAULT_CONFIG = {
    "host": "localhost",
    "database": "easytrade_db",
    "user": "easytrade",
    "password": "masterkey",
    "check_time": 10,
    "markup": 5,
    "weight_table_name": "gd_articul"
}

def get_date():
    now = datetime.now()
    return now.strftime("%d.%m.%Y %H:%M:%S")

def write_log_file(text):
    with open(log_file, "a", encoding='utf-8') as file:
        formatted_text = f"{get_date()} - {text}\n"
        file.write(formatted_text)
        print(formatted_text)

def configure_settings(data_dict=DEFAULT_CONFIG, filename="config.json"):
    if os.path.exists(filename):
        try:
            with open(filename, 'r', encoding='utf-8') as json_file:
                data_dict = json.load(json_file)
            return data_dict
        except FileNotFoundError:
            write_log_file(f"Error: File '{filename}' not found")

        except json.JSONDecodeError:
            write_log_file(f"Error: File '{filename}' contains invalid JSON")
            os.remove(filename)

        except Exception as e:
            write_log_file(f"Error reading JSON file: {e}")
            os.remove(filename)


    try:
        with open(filename, 'w', encoding='utf-8', errors="replace") as json_file:
            json.dump(data_dict, json_file, indent=4, ensure_ascii=False)
    except Exception as e:
        write_log_file(f"Error writing to JSON file: {e}")
    else:
        return data_dict


def value_to_float(s):
    """
    Try to convert a string to float.

    Parameters:
        s: Input string to convert

    Returns:
        float: If the string can be converted to a number otherwise 1
    """
    if s is False:
        return 1.0

    try:
        return float(s)
    except (ValueError, TypeError):
        return 1.0
