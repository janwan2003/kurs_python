import json

class Item:
    def __init__(self, title, creator, year):
        self.title = title
        self.creator = creator
        self.year = year

    # Method to print all the classed to prevent redundancy,
    # if any special suffixes like minutes or (ISBN) needed it can
    # be provided in dictionary. 
    def display_info(self, special_suffixes={}):
        for attribute, value in self.__dict__.items():
            suffix = special_suffixes.get(attribute, "")
            print(f"{attribute.capitalize()}: {value}{suffix}")
        print()

class Book(Item):
    def __init__(self, title, creator, year, genre, isbn):
        super().__init__(title, creator, year)
        self.genre = genre
        self.isbn = isbn

    def display_info(self):
        super().display_info(special_suffixes={"isbn": " (ISBN)"})

class Movie(Item):
    def __init__(self, title, creator, year, genre, duration):
        super().__init__(title, creator, year)
        self.genre = genre
        self.duration = duration

    def display_info(self):
        super().display_info(special_suffixes={"duration": " minutes"})


class Library:
    def __init__(self):
        self.items = []

    def add_item(self, item):
        self.items.append(item)

    def display_items(self):
        for item in self.items:
            item.display_info()

    def save_to_file(self, filename):
        with open(filename, 'w') as file:
            json_data = [{'type': type(item).__name__, 'data': item.__dict__} for item in self.items]
            json.dump(json_data, file, indent=4)

    def load_from_file(self, filename):
        with open(filename, 'r') as file:
            json_data = json.load(file)
            for item_data in json_data:
                item_class = globals()[item_data['type']]
                self.add_item(item_class(**item_data['data']))

def recommend_movies(library, genre):
    print(f"Movies similar to genre '{genre}':")
    for item in library.items:
        if isinstance(item, Movie) and item.genre == genre:
            print(f"- {item.title} by {item.creator}, {item.year}")

# Example usage
library = Library()
library.add_item(Book("Potop", "Henryk Sienkiewicz", 1886, "Historical Novel", "978-3-16-148410-0"))
library.add_item(Movie("Seksmisja", "Juliusz Machulski", 1984, "Sci-Fi", 117))
library.add_item(Movie("Kongres", "Ari Folman", 2013, "Sci-Fi", 122))
library.add_item(Movie("Smoleńsk", "Antoni Krauze", 2016, "Mystery", 120))

library.display_items()

recommend_movies(library, "Sci-Fi")
