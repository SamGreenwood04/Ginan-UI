from ruamel.yaml import YAML
from pathlib import Path

yaml = YAML() # Create the parser instance
yaml.preserve_quotes = True # Do not remove quotes around values
yaml.width = 4096
yaml.indent(mapping=4, sequence=4, offset=4) # Preserve indenting through the file

def load_yaml(file_path: str) -> dict:
    """
    Load a YAML file and return its contents as a dictionary-like structure.

    :param file_path: The path to the YAML file to load (e.g. "/data/resources/ppp_example.yaml")
    :return: Dictionary-like structure containing the contents of the YAML file.
    """
    path = Path(file_path)
    data = yaml.load(path.read_text()) # Create a dictionary-like structure
    return data

def write_yaml(file_path: str, config):
    path = Path(file_path)
    yaml.dump(config, path.open('w'))

def update_yaml_values(file_path: str, updates: list[tuple[str, str]]):
    """
    Modify several YAML keys in the provided file path to new values.

    :param file_path: The path to the YAML file to modify (e.g. "/data/resources/ppp_example.yaml")
    :param updates: List of (key_path, new_value) tuples, e.g.
                    [("outputs.outputs_root", "new/path"),
                     ("inputs.inputs_root", "other/path")]
    """
    path = Path(file_path)
    data = yaml.load(path.read_text()) # Create a dictionary-like structure

    # Iterate through each modification
    for key_path, new_value in updates:
        # Move through the provided dotted path
        keys = key_path.split(".")
        node = data
        for k in keys[:-1]: # Iterate to the key before the one we want to edit
            if k not in node:
                raise KeyError(f"Path segment '{k}' not found in {key_path}")
            node = node[k]

        last_key = keys[-1]
        if last_key not in node:
            raise KeyError(f"Final key '{last_key}' not found in {key_path}")

        # Finally, make the change to the specified key-value pair
        node[last_key] = new_value

    yaml.dump(data, path.open("w")) # Open file_path with write permission and dump in the changes