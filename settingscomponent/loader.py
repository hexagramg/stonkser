import yaml
import typing as t
import asyncio

def load_settings(filename: str):
    with open(filename, mode='r') as file:
        settings = yaml.load(file)
    return settings


NAME = 'settings.yaml'
SETTINGS = load_settings(NAME)
loop = asyncio.get_event_loop()
SEQURITIES_NAME = 'ss.yaml'


SEQURITIES = load_settings(SEQURITIES_NAME)
