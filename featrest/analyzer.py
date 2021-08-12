from multiprocessing import (Process, Pipe)

from featuretools.mkfeat.feat_importance import FeatureImportance
from featuretools.mkfeat.error import Error


class Analyzer(FeatureImportance):
    def __init__(self):
        super().__init__()
        self._proc = None
        self._conn = None
        self._prog = None
        self._importance = None

    def _progress_handler(self, prog):
        self._conn.send(prog)

    def _analyze_func(self, path_data, columns_data, path_label, columns_label, conn):
        self._conn = conn
        err = self.load(path_data, columns_data, path_label, columns_label)
        conn.send(err)
        if err != Error.OK:
            return
        self.analyze(self._progress_handler)
        conn.send(super().get_importance())

    def start(self, path_data, columns_data, path_label, columns_label):
        conn_parent, conn_child = Pipe()
        self._proc = Process(target=self._analyze_func,
                             args=(path_data, columns_data, path_label, columns_label, conn_child))
        self._proc.start()
        self._conn = conn_parent
        return conn_parent.recv()

    def stop(self):
        if self._proc is None:
            return
        self._proc.terminate()
        self.cleanup()

    def cleanup(self):
        if self._proc is not None:
            self._proc.join()
            # TODO: close is supported >= 3.7. Is it required?
            # self._proc.close()
            self._proc = None

    def get_importance(self):
        if self.is_running():
            return Error.ERR_ONGOING
        self._deplete_progress()
        if self._importance is None:
            return Error.ERR_GENERAL
        return self._importance

    def is_completed(self):
        if self._importance is not None:
            return True
        if self._proc is None:
            return False
        self._deplete_progress()
        return self._importance is not None

    def is_running(self):
        if self._proc is None:
            return False
        self._deplete_progress()
        return self._importance is None

    def get_progress(self):
        self._deplete_progress()
        return self._prog

    def _deplete_progress(self):
        try:
            while self._conn.poll():
                prog = self._conn.recv()
                if isinstance(prog, int):
                    self._prog = prog
                else:
                    self._importance = prog
        except EOFError:
            pass
