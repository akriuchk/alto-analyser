import logging
from typing import Iterator
from lxml import etree

from config import Config

config = Config()

default_str_attr: str = "CONTENT"
full_str_attr: str = "SUBS_CONTENT"
word_confidence_attr: str = "WC"

confidence_sum: float = 0.0
word_count: int = 0

log = logging.getLogger('alto.text_processor')

def extract_text_lines(xml: bytes) -> Iterator[str]:
    log.debug(f"read str into XML dom str:{xml[:240]}")
    root = etree.XML(xml)
    text_lines_elems: list = root.findall(".//TextLine")

    if len(text_lines_elems) == 0:
        # log print(f"parsing of ... skipped")
        yield ""

    for line in text_lines_elems:
        sentence: list[str] = []
        for string_elem in line.findall("String"):
            if default_str_attr not in string_elem.attrib:
                log.info(f"Warn: CONTENT attr not found on {string_elem}")
                continue
            if (full_str_attr in string_elem.attrib
                    and (len(sentence) == 0 or string_elem.attrib[full_str_attr] != sentence[-1])):
                if float(string_elem.attrib[word_confidence_attr]) > config.alto_confidence:
                    sentence.append(string_elem.attrib[full_str_attr])
                    add_confidence(string_elem)
                else:
                    sentence.append("<redacted>")

            if default_str_attr in string_elem.attrib:
                if float(string_elem.attrib[word_confidence_attr]) > config.alto_confidence:
                    sentence.append(string_elem.attrib[default_str_attr])
                    add_confidence(string_elem)
                else:
                    sentence.append("<redacted>")

        yield " ".join(sentence)


def add_confidence(string_elem):
    global confidence_sum, word_count
    word_conf = float(string_elem.attrib[word_confidence_attr])
    confidence_sum += word_conf
    word_count += 1
    # print(f'{confidence_sum} += {word_conf}')


def avg_confidence() -> float:
    log.debug(f'{confidence_sum}/{word_count} * 100%')
    return (confidence_sum / word_count) * 100
