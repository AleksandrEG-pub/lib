import logging
import random
from datetime import datetime, date, timedelta
from typing import Any, Callable, Dict, List
from faker import Faker

fake = Faker()

DOMAINS = ['gmail.com', 'yandex.com', 'mail.com', 'example.com'],


def rnd_name():
    f_name = fake.first_name()
    l_name = fake.last_name()
    return f"{f_name} {l_name}"

def rnd_first_name():
    return fake.first_name()

def rnd_last_name():
    return fake.last_name()

def rnd_title():
    words: list = fake.words(random.randint(1, 5))
    words.append(str(random.randint(1, int(10e6))))
    return ' '.join(words).title()

def rnd_email():
    f_name = fake.first_name()
    l_name = fake.last_name()
    num = random.randint(1, int(10e10))
    domain = fake.domain_name()
    return f"{f_name}.{num}.{l_name}@{domain}"

def rnd_birthday(start_date: datetime | str = '-25y',
                 end_date: datetime | str = 'today') -> datetime.date:
    return fake.date_between(start_date=start_date, end_date=end_date)

def rnd_date(start_date: datetime | str = '-25y',
             end_date: datetime | str = 'today') -> datetime.date:
    return fake.date_between(start_date=start_date, end_date=end_date)

def create_isbn_generator(start_at=1000000000):
    """Create an ISBN generator that remembers its position"""
    current = start_at
    def get_next():
        nonlocal current
        isbn = f"ISBN-{current}"
        current += 1
        return isbn
    return get_next

def generate_batches(
    total_items: int,
    batch_size: int,
    create_item_function: Callable[[], Any],
    after_batch_function: Callable[[list], None]=None
):
    """
    Generate items in batches
    
    Args:
        total_items: How many items to create in total
        batch_size: How many items per batch
        create_item_function: Function that creates ONE item
        after_batch_function: Function to run after each batch (optional)
    
    Returns:
        List of batches (each batch is a list of items)
    """
    all_batches = 0
    if batch_size == 0:
        batch_size = 1
    full_batches = total_items // batch_size
    last_batch_size = total_items % batch_size
    print(f"Generating {total_items} items in {full_batches + (1 if last_batch_size > 0 else 0)} batches...")
    for batch_num in range(full_batches):
        print(f"  Batch {batch_num + 1}: Creating {batch_size} items...")
        batch = []
        for i in range(batch_size):
            item = create_item_function()
            batch.append(item)
        all_batches += 1
        if after_batch_function:
            try:
                after_batch_function(batch)
            except Exception as e:
                error_message = str(e).split('\n')[0]
                logging.error(f"failed to execute after batch function: {type(e).__name__}: {error_message}")
    if last_batch_size > 0:
        print(f"  Final batch: Creating {last_batch_size} items...")
        batch = []
        for i in range(last_batch_size):
            item = create_item_function()
            batch.append(item)
        all_batches += 1
        if after_batch_function:
            try:
                after_batch_function(batch, full_batches + 1)
            except Exception as e:
                error_message = str(e).split('\n')[0]
                logging.error(f"failed to execute after last batch function: {type(e).__name__}: {error_message}")
    print(f"Done! Generated {total_items} items in {all_batches} batches.")
    