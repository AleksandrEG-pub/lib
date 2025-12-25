from typing import Dict, List
import datetime as dt_module
import database.data_generator as generate_random_data


def make_reader() -> Dict:
    return {
        'first_name': generate_random_data.rnd_first_name(),
        'last_name': generate_random_data.rnd_last_name(),
        "email": generate_random_data.rnd_email()
    }
