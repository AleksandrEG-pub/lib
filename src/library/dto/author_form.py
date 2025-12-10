from dataclasses import dataclass


@dataclass
class AuthorForm:
    first_name: str
    last_name: str
    birth_date: str
    email: str
