import zipfile
from os import PathLike
from typing import Iterator


def extract_xml_pages_from_zip(zip_path: PathLike[str]) -> Iterator[str]:
    print(f"Open decade-zip file '{zip_path}'")
    with zipfile.ZipFile(zip_path, 'r') as decade_zip:
        for book_zip in decade_zip.infolist():
            if not book_zip.filename.endswith(".zip"):
                continue
            print(f"Open book-zip file '{book_zip.filename}'")
            with decade_zip.open(book_zip.filename) as nested_zip_file:
                with zipfile.ZipFile(nested_zip_file, 'r') as opened_book:
                    for zipped_entity in opened_book.infolist():
                        if not zipped_entity.is_dir() and zipped_entity.filename.endswith('.xml'):
                            with opened_book.open(zipped_entity.filename, 'r') as f:
                                #LOG print(f"Read xml(page) from {zipped_entity.filename}")
                                yield f.read()