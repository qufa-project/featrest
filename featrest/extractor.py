from multiprocessing import (Process, Pipe)
import threading
import typing

from featuretools.mkfeat.feat_extractor import FeatureExtractor
from featuretools.mkfeat.error import Error

from errpage import ErrorSvc


class Extractor(FeatureExtractor):
    def __init__(self, path, columns):
        super().__init__(path, columns, self._progress_handler)
        self._proc = None
        self._conn = None
        self._progListener: typing.Optional[ProgListener] = None

    def _progress_handler(self, prog):
        self._conn.send(prog)

    def _msgloop(self):
        while True:
            msg = self._conn.recv()
            if msg[0] == "save":
                super().save(msg[1])
            elif msg[0] == "featureinfo":
                self._conn.send(super().get_feature_info())
            elif msg[0] == "exit":
                break

    def _extractor_func(self, operators, conn):
        self._conn = conn
        err = self.extract_features(operators)
        if err != Error.OK:
            self._conn.send(err)
        else:
            self._msgloop()

    def start(self, operators):
        conn_parent, conn_child = Pipe()
        self._proc = Process(target=self._extractor_func, args=(operators, conn_child))
        self._proc.start()
        self._conn = conn_parent
        self._progListener = ProgListener(conn_parent)
        self._progListener.start()
        self._progListener.join(0.5)
        if self._is_running() or self._is_completed():
            return Error.OK
        return self._progListener.prog

    def save(self, path) -> Error:
        if self._is_running():
            return Error.ERR_ONGOING
        if not self._is_completed():
            return ErrorSvc.ERR_STOPPED
        self._conn.send(["save", path])
        return Error.OK

    def get_feature_info(self):
        if self._is_running():
            return Error.ERR_ONGOING
        if not self._is_completed():
            return None
        self._conn.send(["featureinfo"])
        return self._conn.recv()

    def stop(self):
        if self._proc is None:
            return ErrorSvc.ERR_STOPPED
        self._proc.terminate()
        self._proc.join(1)
        if self._proc.is_alive():
            return Error.ERR_ONGOING
        self._proc = None
        if self._is_completed():
            return ErrorSvc.ERR_COMPLETED
        return Error.OK

    def cleanup(self):
        if self._proc is None:
            return Error.OK

        if self._is_running():
            return Error.ERR_ONGOING
        self._conn.send(["exit"])
        self._proc.join(5)
        # TODO: close is supported >= 3.7. Is it required?
        # self._proc.close()
        if self._proc.is_alive():
            return Error.ERR_ONGOING
        self._proc = None
        return Error.OK

    def _is_completed(self):
        return self._progListener.prog == 100

    def _is_stopped(self):
        return not self._progListener.is_alive() and self._progListener.prog != 100

    def _is_running(self):
        return self._progListener.is_alive() and self._progListener.prog < 100

    def get_progress(self):
        if self._is_stopped():
            return None
        return self._progListener.prog


class ProgListener(threading.Thread):
    def __init__(self, conn):
        super().__init__()
        self.prog = 0
        self._conn = conn

    def run(self):
        while True:
            try:
                prog = self._conn.recv()
                self.prog = prog
                if isinstance(prog, int):
                    if prog == 100:
                        break
                else:
                    break
            except EOFError:
                break
