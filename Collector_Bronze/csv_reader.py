import pandas as pd

class CsvReader:
    def __init__(self):
        self.data = None

    def read_csv(self, file_path):
        self.data = pd.read_csv(file_path)
        #print(self.data)
        return self.data