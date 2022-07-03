import csv


# Read rows from csv file into Dictionary
def read_csv(data_set):
    '''Takes in a file name; returns a list of dictionaries'''
    rows = []
    with open(data_set, "r") as f:
        reader = csv.DictReader(f)  # read rows into a dictionary format
        for row in reader:
            rows.append(row)
        return rows