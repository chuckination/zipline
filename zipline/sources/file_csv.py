from os import access, R_OK
from os.path import isfile

from pandas import read_csv

from zipline.sources.requests_csv import {
    roll_dts_to_midnight,
    FetcherEvent,
    ALLOWED_READ_CSV_KWARGS,
    PandasCSV,
}

logger = Logger('CSV File Source Logger')

class PandasFileCSV(PandasCSV):
    # maximum 100 megs to prevent DDoS
    MAX_DOCUMENT_SIZE = (1024 * 1024) * 100

    # maximum number of bytes to read in at a time
    CONTENT_CHUNK_SIZE = 4096

    def __init__(self,
                 filepath,
                 pre_func,
                 post_func,
                 asset_finder,
                 trading_day,
                 start_date,
                 end_date,
                 date_column,
                 date_format,
                 timezone,
                 symbol,
                 mask,
                 symbol_column,
                 data_frequency,
                 country_code,
                 special_params_checker=None,
                 **kwargs):

        self.filepath = filepath

        self.namestring = type(self).__name__

        super(PandasFileCSV, self).__init__(
            pre_func,
            post_func,
            asset_finder,
            trading_day,
            start_date,
            end_date,
            date_column,
            date_format,
            timezone,
            symbol,
            mask,
            symbol_column,
            data_frequency,
            country_code=country_code,
            **kwargs
        )

        self.df = self.load_df()

        self.special_params_checker = special_params_checker

    def fetch_data(self):
        # create a data frame directly from passing the filepath to
        # pandas read_csv
        if not isfile(self.filepath) or not access(self.filepath, R_OK):
            raise Exception('{} is not a valid file for reading'.format(
                self.filepath))

        try:
            # see if pandas can parse csv data
            frames = read_csv(self.filepath, **self.pandas_kwargs)

            frames_hash = hashlib.md5(str(fd.getvalue()).encode('utf-8'))
            self.fetch_hash = frames_hash.hexdigest()
        except pd.parser.CParserError:
            # could not parse the data, raise exception
            raise Exception('Error parsing local CSV file data.')
        finally:
            fd.close()

        return frames
