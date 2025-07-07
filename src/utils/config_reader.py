import configparser
import os

def load_config(marketname: str):
    config_path = os.path.join("config", marketname, "config.ini")
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file for market '{marketname}' not found at: {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config
