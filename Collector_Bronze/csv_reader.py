#pip install pandas
#
import pandas as pd

class CsvReader:
    def __init__(self, data):
        self.data = data

    def read_csv(self, file_path):
        self.data = pd.read_csv(file_path)
        return self.data