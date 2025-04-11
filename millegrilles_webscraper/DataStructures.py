from typing import Optional, TypedDict


class AttachedFile(TypedDict):
    fuuid: str
    format: str
    nonce: str
    cle_id: Optional[str]
    compression: Optional[str]


class AttachedFileInterface:

    def encrypt_upload_file(self, secret_key: bytes, fp) -> AttachedFile:
        """
        Encrypts and uploads a file to the filehost
        :param secret_key: Secret encryption key
        :param fp: File handle at the proper position for reading content
        :return:
        """
        raise NotImplementedError('interface method - must override')


class CustomProcessOutput:

    def __init__(self):
        self.files: Optional[list[AttachedFile]] = None
        self.pub_date_start: Optional[int] = None
        self.pub_date_end: Optional[int] = None


class DataCollectorTransaction(TypedDict):
    feed_id: str
    data_id: str
    save_date: int
    data_fuuid: str
    key_ids: list[str]
    pub_date_start: Optional[int]
    pub_date_end: Optional[int]
    attached_fuuids: Optional[list[str]]


class DataFeedFile(TypedDict):
    feed_id: str
    data_id: str
    save_date: int
    encrypted_data: dict
    pub_start_date: Optional[int]
    pub_end_date: Optional[int]
    files: Optional[list[AttachedFile]]


class Filehost:

    def __init__(self, filehost_id: str):
        self.filehost_id = filehost_id
        self.url_internal: Optional[str] = None
        self.url_external: Optional[str] = None
        self.tls_external: Optional[str] = None
        self.instance_id: Optional[str] = None
        self.deleted: Optional[bool] = None
        self.sync_active: Optional[bool] = None

    @staticmethod
    def load_from_dict(value: dict):
        filehost_id = value['filehost_id']
        filehost = Filehost(filehost_id)

        filehost.url_internal = value.get('url_internal')
        filehost.url_external = value.get('url_external')
        filehost.tls_external = value.get('tls_external')
        filehost.instance_id = value.get('instance_id')
        filehost.deleted = value.get('deleted')
        filehost.sync_active = value.get('sync_active')

        return filehost

    @staticmethod
    def init_new(filehost_id: str, instance_id: str, url: str):
        filehost = Filehost(filehost_id)
        filehost.url_internal = url
        filehost.instance_id = instance_id
        filehost.deleted = False
        filehost.sync_active = True
        return filehost
