from multiprocessing import (Process, Pipe)

from featuretools.mkfeat.feat_importance import FeatureImportance
from featuretools.mkfeat.error import Error

import threading


class Analyzer(threading.Thread):
    def __init__(self, path_data, columns_data, path_label, columns_label):
        super().__init__()
        self._path_data = path_data
        self._columns_data = columns_data
        self._path_label = path_label
        self._columns_label = columns_label
        self._impt = None
        self._prog = None
        self._stopping = False
        self._importance = None

    def _progress_handler(self, prog):
        self._prog = prog
        if self._stopping:
            return True
        return False

    def run(self):
        self._impt.analyze(self._progress_handler)

    def start(self) -> Error:
        self._impt = FeatureImportance()
        err = self._impt.load(self._path_data, self._columns_data, self._path_label, self._columns_label)
        if err != Error.OK:
            return err

        super().start()
        return Error.OK

    def stop(self):
        if not self.is_alive():
            return Error.ERR_STOPPED
        self._stopping = True
        self.join(30)
        if self.is_alive():
            return Error.ERR_ONGOING
        return Error.OK

    def cleanup(self):
        if self.is_alive():
            return Error.ERR_ONGOING
        self.join()
        return Error.OK

    def get_importance(self):
        if self.is_alive():
            return Error.ERR_ONGOING
        if self._prog != 100:
            return Error.ERR_STOPPED
        return self._impt.get_importance()

    def get_progress(self):
        if self._prog == 100 or self.is_alive():
            return self._prog
        return 0
