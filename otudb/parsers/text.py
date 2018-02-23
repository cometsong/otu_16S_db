import attr

from ..utils import log_it, now


log = log_it(logname='otudb.parser')


@attr.s
class TextParser(object):
    """Parse text files; open, read, write, and have fun with."""
    filename: str = attr.ib()
    mode: str = attr.ib(default='w+')
    headers: list = attr.ib(init=False, default='[]')
    _fh = attr.ib(init=False)

    @_fh.default
    def get_fh(self):
        return self.open_file()


    def open_accessible_file(self):
        """determine if file can be accessed in the specified mode.
        Else check writeable, then readable to determine if at all.
        Return file object handle.
        """
        try:
            _fh = open(self.filename, self.mode)
            log.debug(f'{self.filename} is open using mode "{self.mode}".')
            return _fh
        except Exception as e:
            log.exception(f'There is a problem opening file: {self.filename} using mode "{self.mode}".')

            try:
                _fh = open(self.filename, 'w+')
                log.debug(f'{self.filename} is open and read/writable.')
                return _fh
            except:

                try:
                    _fh = open(self.filename, 'r')
                    log.debug(f'{self.filename} is open and read-only.')
                    return _fh
                except Exception as e:
                    log.exception(f'There is a problem opening file: {self.filename}')
                    raise e


    def open_file(self, filename: str=None, mode=None):
        """open filename as a text file"""
        self.filename = filename if filename else self.filename
        self.mode = mode if mode else self.mode
        return self.open_accessible_file()


    def load_data(self):
        """yield rows from file using readline"""
        log.info(f'Loading rows from {self.filename}')
        try:
            for row in self._fh:
                yield row
        except Exception as e:
            log.exception(f'Reading file {self.filename}: {e!s}')


    def write_row(self, row: str):
        """write string of data to filename"""
        if not row:
            log.exception(f'Missing row for {self.filename}')
            return False
        else:
            row = str(row) # just to make sure....
        try:
            # log.info(f'Writing row to {self.filename}')
            log.debug(f'Contents of row: {row!s}')
            return self._fh.write(row)
        except IOError as e:
            log.exception(f'Error writing row to file, {e!s}')
            raise e


    def write_headers(self, row, delimiter=None):
        """write all column headers to filename"""
        if not row:
            log.exception(f'Missing headers for {self.filename}')
            return False
        try:
            if delimiter and isinstance(row, list):
                row = delimiter.join(row)
            log.info(f'Writing headers to {self.filename}')
            return self.write_row(row)
        except IOError as e:
            log.exception(f'Error writing headers to file {self.filename}, {e!s}')
            raise e


    def write(self, headers: list=[], values: list=[], delimiter=None):
        """write all headers and values in possibly delimited
        format to outfile.

        Values is list of lists with fields in (hopefully) in same 
        order as the list of headers.

        To write only header to file, pass 'headers' and omit `values`.
        To write only values to file, pass `values`  and omit 'headers'.
        """
        try:
            if headers:
                self.headers = headers
                self.write_headers(self.headers, delimiter)

            if values:
                log.info(f'Writing values to {self.filename}')
                for row in values:
                    if delimiter and isinstance(row, list):
                        row = delimiter.join(row)
                    self.write_row(row)
        except IOError as e:
            log.exception(f'Error writing to file {self.filename}, {e!s}')
            raise e
        return True

