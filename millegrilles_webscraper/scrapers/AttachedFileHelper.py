import aiohttp
import asyncio
import binascii
import logging
import tempfile

from asyncio import TaskGroup
from typing import Optional, TypedDict
from urllib.parse import urljoin

from millegrilles_messages.chiffrage.Mgs4 import CipherMgs4WithSecret
from millegrilles_messages.messages import Constantes
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.DataStructures import Filehost, AttachedFileInterface, AttachedFile


CONST_GET_FILE_READ_SOCK_TIMEOUT = 20       # Timeout if no data read after 20 seconds
MAX_UPLOAD_SIZE = 100_000_000


class AttachedFileHelper(AttachedFileInterface):

    def __init__(self, context: WebScraperContext):
        self.__logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)
        self.__context = context
        self.__filehost: Optional[Filehost] = None
        self.__filehost_ready = asyncio.Event()
        self.__upload_ready = asyncio.Event()
        self.__session_semaphore = asyncio.BoundedSemaphore(1)
        self.__session: Optional[aiohttp.ClientSession] = None
        self.__filehost_url: Optional[str] = None

    @property
    def ready(self) -> asyncio.Event:
        return self.__upload_ready

    async def stop_thread(self):
        """ Thread that triggers all blocking events on closure """
        await self.__context.wait()
        self.__filehost_ready.set()
        self.__upload_ready.set()

    async def run(self):
        async with TaskGroup() as group:
            group.create_task(self.__maintenance_thread())
            group.create_task(self.__session_thread())

    async def __maintenance_thread(self):
        while self.__context.stopping is False:
            try:
                await self.select_filehost()
            except asyncio.TimeoutError:
                # Retry quickly
                await self.__context.wait(15)
            else:
                await self.__context.wait(300)

    async def __session_thread(self):
        while self.__context.stopping is False:
            filehost = self.__filehost

            if filehost:
                # Configure SSL connection
                if filehost.url_external:
                    filehost_url = filehost.url_external
                    tls_mode = filehost.tls_external or 'external'
                elif filehost.url_internal:
                    filehost_url = filehost.url_internal
                    tls_mode = 'millegrille'
                else:
                    raise ValueError('Unsupported filehost configuration')

                ssl_context = None
                verify = True
                if tls_mode == 'millegrille':
                    ssl_context = self.__context.ssl_context
                elif tls_mode == 'nocheck':
                    verify = False

                self.__filehost_url = filehost_url

                # Configure and open client session
                connector = aiohttp.TCPConnector(ssl_context=ssl_context, verify_ssl=verify)
                client_timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=CONST_GET_FILE_READ_SOCK_TIMEOUT)
                async with aiohttp.ClientSession(connector=connector, timeout=client_timeout) as session:
                    try:
                        while self.__context.stopping is False:
                            async with self.__session_semaphore:
                                await self.authenticate(session)

                            # Session is ready
                            self.__session = session
                            self.__upload_ready.set()

                            self.__logger.info("Authenticated with filehost %s" % filehost_url)
                            # Reauthenticate after a while to keep cookie active
                            await self.__context.wait(600)
                    except aiohttp.ClientError:
                        self.__logger.exception("Error connecting to filehost %s" % filehost_url)

                        # Cleanup before wait
                        self.__session = None  # Ensure session is removed
                        self.__filehost_url = None
                        self.__upload_ready.clear()

                        # Wait to reconnect
                        await self.__context.wait(20)
                    finally:
                        # Cleanup
                        self.__session = None
                        self.__filehost_url = None
                        self.__upload_ready.clear()
            else:
                await self.__filehost_ready.wait()

    async def select_filehost(self):
        producer = await self.__context.get_producer()
        response = await producer.request(dict(), 'CoreTopologie', 'getFilehostForInstance', exchange=Constantes.SECURITE_PUBLIC)
        if response.parsed['ok'] is not True:
            raise ValueError('Error getting filehost: %s' % response.parsed.get('err'))
        filehost_dict = response.parsed['filehost']
        filehost = Filehost.load_from_dict(filehost_dict)

        self.__filehost = filehost
        self.__filehost_ready.set()

    async def authenticate(self, session: aiohttp.ClientSession):
        filehost_url = self.__filehost_url
        if filehost_url is None:
            raise ValueError('Filehost not selected')

        url_authenticate = urljoin(filehost_url, '/filehost/authenticate')
        auth_message = self.__get_auth_message()
        async with session.post(url_authenticate, json=auth_message) as r:
            r.raise_for_status()

    def __get_auth_message(self):
        ca = self.__context.ca
        auth_message, message_id = self.__context.formatteur.signer_message(
            Constantes.KIND_COMMANDE, dict(), 'filehost', action='authenticate')
        auth_message['millegrille'] = ca.certificat_pem
        return auth_message

    async def encrypt_upload_file(self, secret_key: bytes, fp) -> AttachedFile:
        # Encrypt content to temporary output
        cipher = CipherMgs4WithSecret(secret_key)
        with tempfile.TemporaryFile() as tmp_output:
            await asyncio.to_thread(_encrypt_file, cipher, fp, tmp_output)

            # Prepare metadta
            fuuid = cipher.hachage
            if fuuid is None:
                raise ValueError('cipher digest was not provided')

            file_size = cipher.taille_chiffree
            nonce = binascii.b2a_base64(cipher.header, newline=False).decode('utf-8').replace('=', '')

            attached_file: AttachedFile = {'fuuid': fuuid, 'cle_id': None, 'format': 'mgs4', 'nonce': nonce}

            # Upload content
            tmp_output.seek(0)  # Rewind file to beginning
            async with self.__session_semaphore:
                await _upload_content(self.__session, self.__filehost_url, fuuid, file_size, tmp_output)

        return attached_file


def _encrypt_file(cipher, src, dest):
    while True:
        chunk = src.read(64 * 1024)
        if len(chunk) == 0:
            break
        dest.write(cipher.update(chunk))

    # Finalizer l'ecriture
    dest.write(cipher.finalize())


async def _upload_content(session: aiohttp.ClientSession, filehost_url: str, fuuid: str, file_size: int, fp):
    # One shot upload
    headers = {'x-fuuid': fuuid, 'Content-Length': str(file_size)}
    upload_url = urljoin(filehost_url, f'/filehost/files/{fuuid}')
    response = await session.put(upload_url, headers=headers, data=fp)
    response.raise_for_status()
