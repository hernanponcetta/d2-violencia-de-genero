import json
from kedro.io.core import AbstractDataSet, DataSetError


class JsonDataSet(AbstractDataSet):
    def __init__(self, filepath):
        self._filepath = filepath

    def _save(self, data):
        with open(self._filepath, "w") as outfile:
            outfile.write(json.dumps(data, indent=4))

    def _load(self):
        raise DataSetError("Write Only DataSet")

    def _describe(self):
        return dict(filepath=self._filepath)
