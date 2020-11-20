import io

from fuzzysearch import find_near_matches
from pdfminer.pdfpage import PDFPage
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.converter import TextConverter
from kedro.io.core import AbstractDataSet, DataSetError

class PdfDataSet(AbstractDataSet):
    def __init__(self, filepath):
        self._filepath = filepath

    def _save(self, _):
        raise DataSetError("Read Only DataSet")

    def _load(self):

        text_list = []

        with open(self._path, 'rb') as f:
        for page in PDFPage.get_pages(f, caching=True, check_extractable=True):
                    resource_manager = PDFResourceManager()
                    fake_file_handle = io.StringIO()
                    converter = TextConverter(resource_manager, fake_file_handle)
                    page_interpreter = PDFPageInterpreter(resource_manager, converter)
                    page_interpreter.process_page(page)
                    text = fake_file_handle.getvalue()               
                    text_list.append(text)

        return text_list

    def _describe(self):
        return dict(filepath = self._filepath)