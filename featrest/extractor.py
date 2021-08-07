from multiprocessing import (Process, Pipe)

from featuretools.mkfeat.feat_extractor import FeatureExtractor
from featuretools.mkfeat.error import Error


class Extractor(FeatureExtractor):
    def __init__(self):
        super().__init__()
        self.proc = None
        self.conn = None

    def _progress_handler(self, prog):
        self.conn.send(prog)

    def _msgloop(self):
        while True:
            msg = self.conn.recv()
            if msg[0] == "save":
                super().save(msg[1])
            elif msg[0] == "featureinfo":
                self.conn.send(super().get_feature_info())
            elif msg[0] == "exit":
                break

    def _extractor_func(self, path, columns, operators, conn):
        self.conn = conn
        err = self.load(path, columns)
        conn.send(err)
        if err != Error.OK:
            return
        self.extract_features(operators, self._progress_handler)
        self._msgloop()

    def start(self, path, columns, operators):
        self._prog = 0
        conn_parent, conn_child = Pipe()
        self.proc = Process(target=self._extractor_func, args=(path, columns, operators, conn_child))
        self.proc.start()
        self.conn = conn_parent
        return conn_parent.recv()

    def save(self, path):
        self.conn.send(["save", path])

    def get_feature_info(self):
        if self._prog != 100:
            return Error.ERR_ONGOING
        self.conn.send(["featureinfo"])
        return self.conn.recv()

    def stop(self):
        if self.proc is None:
            return
        self.proc.terminate()
        self.cleanup()

    def cleanup(self):
        if self.proc is not None:
            self.conn.send(["exit"])
            self.proc.join()
            # TODO: close is supported >= 3.7. Is it required?
            # self.proc.close()
            self.proc = None

    def is_completed(self):
        if self._prog == 100:
            return True
        if self.proc is None:
            return False
        self._deplete_progress()
        return self._prog == 100

    def is_running(self):
        if self.proc is None:
            return False
        self._deplete_progress()
        return self._prog < 100

    def get_progress(self):
        self._deplete_progress()
        return self._prog

    def _deplete_progress(self):
        while self.conn.poll():
            prog = self.conn.recv()
            if isinstance(prog, int):
                self._prog = prog
