import configparser

def get_keys(section: str, key: str) -> str: 

    """Parses through a conf file and returns the keys needed from a given section."""

    parser = configparser.ConfigParser()

    parser.read("data/keys.conf")
    key = parser.get(section + "_credentials", key)
    
    return key