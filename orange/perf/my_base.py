from types import SimpleNamespace
my_stuff = SimpleNamespace()
country = 'em'
my_stuff.file_name = 'new1.csv'
my_stuff.folder_name = f'{country}_images'
my_stuff.bucket_folder_name = f'design_db/{country.upper()}_high'
my_stuff.completed = f'completed_{country}.txt'
