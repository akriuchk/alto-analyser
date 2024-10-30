from unittest import TestCase

from processor.xml_processor import extract_text_lines


class Test(TestCase):
    def test_extract_text_line(self):
        xml = read_file_into_string("../test_data/ALTO/000001280_000001.xml")
        for text_line in extract_text_lines(xml):
            print(text_line)

def read_file_into_string(path: str) -> str:
    with open(path, 'r') as file:
       return file.read()