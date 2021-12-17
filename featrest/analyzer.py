from featuretools.mkfeat.feat_importance import FeatureImportance
from featuretools.mkfeat.error import Error

import threading

from .errpage import ErrorSvc
from . import util


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
        err = self._impt.analyze()
        util.remove(self._path_data)
        util.remove(self._path_label)
        if err != Error.OK:
            self._prog = err

    def start(self) -> Error:
        self._impt = FeatureImportance(self._path_data, self._columns_data, self._path_label, self._columns_label,
                                       self._progress_handler)

        super().start()
        self.join(1.5)
        if self.is_alive() or self._prog == 100:
            return Error.OK
        return self._prog

    def stop(self):
        if not self.is_alive():
            return ErrorSvc.ERR_STOPPED
        self._stopping = True
        self.join(30)
        if self.is_alive():
            return Error.ERR_ONGOING
        util.remove(self._path_data)
        util.remove(self._path_label)
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
            return ErrorSvc.ERR_STOPPED
        return self._impt.get_importance()

    def get_progress(self):
        if self.is_alive():
            if self._prog is None:
                return 0
            return self._prog
        elif self._prog == 100:
            return 100
        return None
