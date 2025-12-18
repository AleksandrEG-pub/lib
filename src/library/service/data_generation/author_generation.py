from typing import Dict, List
import datetime as dt_module
import database.data_generator as generate_random_data


def make_author() -> Dict:
    date_of_birth = generate_random_data.rnd_date(
        dt_module.date(1678, 1, 1), dt_module.date(2000, 12, 31))
    return {
        'first_name': generate_random_data.rnd_first_name(),
        'last_name': generate_random_data.rnd_last_name(),
        'birth_date': date_of_birth.isoformat()
    }
