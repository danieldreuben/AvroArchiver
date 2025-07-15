import sys
from fastavro import reader

def read_avro_file(filename):
    with open(filename, 'rb') as fo:
        for record in reader(fo):
            print(record)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python dump.py <filename>")
    else:
        read_avro_file(sys.argv[1])

