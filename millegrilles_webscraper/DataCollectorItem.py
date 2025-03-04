from typing import TypedDict, Optional


class DecryptionInfo(TypedDict):
    cle_id: str
    nonce: str
    format: str

class DataCollectorFilesDict(TypedDict):
    fuuid: str
    decryption: DecryptionInfo

class DataCollectorDict(TypedDict):
    data_id: str
    feed_id: str
    pub_date: int
    encrypted_data: dict
    files: Optional[list[DataCollectorFilesDict]]


class DataCollectorItem:

    def __init__(self, feed_id: str):
        self.__feed_id = feed_id
        self.__data_id: Optional[str] = None

    def get_data_id(self) -> str:
        if not self.__data_id:
            self.__data_id = self._produce_data_id()
        return self.__data_id

    def produce_data(self) -> dict:
        raise NotImplementedError('must be implemented')

    def _produce_data_id(self):
        raise NotImplementedError('must be implemented')
