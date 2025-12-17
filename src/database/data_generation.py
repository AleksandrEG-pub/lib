from datetime import datetime as dt_class, timedelta
import datetime as dt_module
import random
from typing import List, Dict, Optional, Union, Iterator, Any, Callable
from abc import ABC, abstractmethod
from dataclasses import dataclass
import pandas as pd
from faker import Faker

fake = Faker()


# ================ CORE DATA GENERATION FRAMEWORK ====================

@dataclass
class GenerationConfig:
    """Configuration for data generation"""
    total_records: int
    batch_size: int = 1000
    verbose: bool = True
    progress_interval: int = 10  # Show progress every N batches


class DataGenerator(ABC):
    """Abstract base class for data generators"""
    
    def __init__(self, config: GenerationConfig):
        self.config = config
        self.generated_count = 0
    
    @abstractmethod
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        """Generate a single batch of data"""
        pass
    
    @abstractmethod
    def save_batch(self, batch: List[Dict[str, Any]]) -> None:
        """Save a batch of generated data"""
        pass
    
    def generate(self) -> int:
        """Generate all data in batches"""
        self.generated_count = 0
        
        for batch_num, current_batch_size in enumerate(
            self._generate_batch_sizes(), 1
        ):
            # Generate batch
            batch = self.generate_batch(current_batch_size)
            
            # Save batch
            self.save_batch(batch)
            
            # Update counters
            self.generated_count += len(batch)
            
            # Show progress if enabled
            if self.config.verbose and batch_num % self.config.progress_interval == 0:
                self._log_progress(batch_num)
        
        if self.config.verbose:
            print(f"✅ Generated {self.generated_count:,} records")
        
        return self.generated_count
    
    def _generate_batch_sizes(self) -> Iterator[int]:
        """Generate batch sizes for iteration"""
        remaining = self.config.total_records
        while remaining > 0:
            batch_size = min(self.config.batch_size, remaining)
            yield batch_size
            remaining -= batch_size
    
    def _log_progress(self, batch_num: int) -> None:
        """Log generation progress"""
        print(f"  Batch {batch_num}: Generated {self.generated_count:,}/{self.config.total_records:,}")


# ================ UTILITY CLASSES ====================

class CommonUtilities:
    """Shared utilities for data generation"""
    
    # Shared constants
    DOMAINS = ['gmail.com', 'yandex.com', 'mail.com', 'example.com']
    GENRES = ['Fiction', 'Non-Fiction', 'Science', 'Fantasy', 'Mystery',
              'Biography', 'History', 'Romance', 'Technology', 'Children']
    
    @staticmethod
    def generate_email(first_name: str, last_name: str) -> str:
        """Generate a unique email address"""
        return (f"{first_name.lower()}.{random.randint(1, 9999)}.{last_name.lower()}"
                f"{random.randint(1, 9999)}@{random.choice(CommonUtilities.DOMAINS)}")
    
    @staticmethod
    def chunk_list(lst: List, chunk_size: int) -> Iterator[List]:
        """Split list into chunks"""
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]
    
    @staticmethod
    def random_date(start_date: Union[str, dt_class, dt_module.date], 
                   end_date: Union[str, dt_class, dt_module.date]) -> dt_module.date:
        """Generate random date between range"""
        return fake.date_between(start_date=start_date, end_date=end_date)


class SequentialIDGenerator:
    """Generate sequential IDs for fields like ISBN"""
    
    def __init__(self, start: int = 1, prefix: str = ""):
        self.current = start
        self.prefix = prefix
    
    def next(self) -> str:
        """Get next sequential ID"""
        value = self.current
        self.current += 1
        return f"{self.prefix}{value}"


# ================ FIELD GENERATORS ====================

class FieldGenerator:
    """Factory for generating specific field types"""
    
    @staticmethod
    def name(part: str = "full") -> str:
        """Generate names"""
        if part == "first":
            return fake.first_name()
        elif part == "last":
            return fake.last_name()
        return fake.name()
    
    @staticmethod
    def text(min_words: int = 2, max_words: int = 5) -> str:
        """Generate text/titles"""
        words = random.randint(min_words, max_words)
        return ' '.join(fake.words(words)).title()
    
    @staticmethod
    def choice(options: List[Any]) -> Any:
        """Random choice from options"""
        return random.choice(options)
    
    @staticmethod
    def number(min_val: int = 1, max_val: int = 10) -> int:
        """Generate random number"""
        return random.randint(min_val, max_val)
    
    @staticmethod
    def date_range() -> Dict[str, dt_module.date]:
        """Generate date ranges (like loan dates)"""
        loan_date = fake.date_between(start_date='-25y', end_date='today')
        due_date = loan_date + timedelta(days=random.randint(14, 60))
        
        return_date = None
        if random.random() < 0.95:  # 95% returned
            return_date = loan_date + timedelta(days=random.randint(1, 90))
        
        return {
            'loan_date': loan_date,
            'due_date': due_date,
            'return_date': return_date
        }


# ================ EXAMPLE USAGE ====================

if __name__ == "__main__":
    # Example of how to create a custom generator
    class ExampleGenerator(DataGenerator):
        """Example generator implementation"""
        
        def __init__(self, config: GenerationConfig, some_dependency: List[int]):
            super().__init__(config)
            self.dependency = some_dependency
            self.isbn_generator = SequentialIDGenerator(start=178_000_000_000)
        
        def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
            batch = []
            for _ in range(batch_size):
                record = {
                    'title': FieldGenerator.text(2, 4),
                    'author_id': FieldGenerator.choice(self.dependency),
                    'isbn': self.isbn_generator.next(),
                    'year': FieldGenerator.number(1950, 2023),
                    'genre': FieldGenerator.choice(CommonUtilities.GENRES),
                    'copies': FieldGenerator.number(1, 10)
                }
                batch.append(record)
            return batch
        
        def save_batch(self, batch: List[Dict[str, Any]]) -> None:
            # Implementation depends on your persistence layer
            # For example: books.add_book(batch)
            pass
    
    # Example usage
    config = GenerationConfig(
        total_records=1000,
        batch_size=100,
        verbose=True,
        progress_interval=5
    )
    
    # This would be populated from your database
    author_ids = [1, 2, 3, 4, 5]
    
    generator = ExampleGenerator(config, author_ids)
    generated = generator.generate()
    print(f"Total generated: {generated}")