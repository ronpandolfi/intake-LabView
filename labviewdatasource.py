import numpy as np
import intake
from collections import defaultdict
from dask import array as da


def convertDate(s):
    # TODO: convert to datetimes
    print(s)
    return str(s)


class LabViewLogSource(intake.source.base.DataSource):
    container = 'dataframe'
    name = 'LabViewLog'
    version = '0.0.1'
    partition_access = True

    def __init__(self, paths, metadata=None):
        # Do init here with a and b
        super(LabViewLogSource, self).__init__(
            metadata=metadata or {}
        )
        self.npartitions = len(paths)
        self.partitions = paths

    def _get_schema(self):
        types = defaultdict(float)
        types['Time']='U32'

        return intake.source.base.Schema(
            datashape=None,
            dtype=types,
            shape=(None, 51),
            npartitions=1,
            extra_metadata=dict()
        )

    def _get_partition(self, i):
        # Return the appropriate container of data here
        return da.from_array(np.genfromtxt(self.partitions[i],
                       delimiter = '\t',
                       names=True,
                       invalid_raise=False,
                       # converters = {0: lambda s:  convertDate(s)},
                       dtype=tuple([np.dtype('U32')]+[float]*50)),1)

    def read(self):
        # self._load_metadata()
        return da.concatenate([self.read_partition(i) for i in range(self.npartitions)])

    def _close(self):
        # close any files, sockets, etc
        pass



if __name__ == '__main__':
    catalog = LabViewLogSource(['tests/04-01-2018 Data.txt'])
    print(catalog.read()[0].compute())

