import yaml
import typing as t


def load_settings(filename: str):
    with open(filename, mode='r') as file:
        settings = yaml.load(file)
    return settings


NAME = 'settings.yaml'
SETTINGS = load_settings(NAME)
