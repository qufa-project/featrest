import tempfile

def get_tmp_fpath():
    fp = tempfile.NamedTemporaryFile(delete=True)
    tmp_fpath = fp.name
    fp.close()
    return tmp_fpath
