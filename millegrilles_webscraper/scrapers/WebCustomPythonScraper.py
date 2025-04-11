import asyncio
import datetime
import logging
import tempfile
import pytz
import json
import zlib

from typing import Optional

from millegrilles_messages.messages import Constantes
from millegrilles_messages.chiffrage.Mgs4 import chiffrer_mgs4_bytes_secrete
from millegrilles_messages.messages.Hachage import Hacheur, hacher, hacher_fichier
from millegrilles_webscraper.Context import WebScraperContext
from millegrilles_webscraper.DataStructures import DataCollectorTransaction, DataFeedFile, AttachedFile
from millegrilles_webscraper.scrapers.WebScraper import WebScraper, FeedParametersType

CHUNK_SIZE = 1024 * 64


class WebCustomPythonScraper(WebScraper):
    """
    Web Scraper that loads the content of a feed and saves it to a filehost and the DataCollector domain.
    The input content is encrypted and saved in a JSON file, that file is uploaded. It's reference information is
    saved as a transaction in DataCollector.
    """

    def __init__(self, context: WebScraperContext, feed: FeedParametersType, semaphore: asyncio.BoundedSemaphore):
        self.__logger = logging.getLogger(f'{__name__}.{self.__class__.__name__}')
        # Define variables filled by update() before super() call
        self.__processing_method: Optional = None

        super().__init__(context, feed, semaphore)

        pass

    def update(self, parameters: FeedParametersType):
        super().update(parameters)
        custom_process = parameters.get('custom_process')
        if custom_process:
            try:
                self.__processing_method = compile(custom_process, '<string>', 'exec')
            except Exception as e:
                self.__logger.exception("Error parsing custom process")
                raise e
        else:
            self.__processing_method = None

    async def process(self, input_file: tempfile.TemporaryFile, output_file: tempfile.TemporaryFile):
        transaction = await self._parse_and_process_file(input_file)
        input_file.seek(0)  # Reposition input to beginning

        # Optional intermediate processing step
        attached_files: Optional[list[AttachedFile]] = None
        if self.__processing_method:
            # values = {"context": self._context, "encryption_key": self._encryption_key, "transaction": transaction, "input_file": input_file}
            values = {}
            try:
                exec(self.__processing_method, values)
                output = await values['process'](self._context, self._encryption_key, input_file)
                transaction['pub_date_start'] = int(output.pub_date_start.timestamp() * 1000.0)
                transaction['pub_date_end'] = int(output.pub_date_end.timestamp() * 1000.0)
                attached_files = output.files

            except Exception as e:
                self.__logger.exception("Error during custom process")
                raise e

        # Generate the output content for the new DataCollector transaction and for filehost.
        fuuid, file_size = await self._generate_output_content(transaction, input_file, output_file, attached_files)

        # Upload output file to filehost
        output_file.seek(0)
        await self._context.file_handler.upload_file(fuuid, file_size, output_file)

        # Send transaction to DataCollector
        if self.__logger.isEnabledFor(logging.DEBUG):
            self.__logger.debug("Transaction\n%s" % json.dumps(transaction, indent=2))

        # Emit item for saving in the DataCollector domain
        attachments: Optional[dict[str, dict]] = None
        if self._key_command:
            attachments = {'key': self._key_command}

        producer = await self._context.get_producer()
        response = await producer.command(transaction, "DataCollector", "saveDataItemV2",
                                          exchange=Constantes.SECURITE_PUBLIC, attachments=attachments)

        if self._encryption_key_submitted is False and response.parsed['ok'] is True:
            # Key saved successfully
            self._key_command = None
            self._encryption_key_submitted = True

    async def _parse_and_process_file(self, input_file: tempfile.TemporaryFile) -> DataCollectorTransaction:
        """
        This is the main processing step.
        :param input_file:
        :return:
        """
        digester = Hacheur('blake2s-256', 'base64')
        while True:
            chunk = await asyncio.to_thread(input_file.read, CHUNK_SIZE)
            if not chunk:
                break
            digester.update(chunk)
        data_digest = digester.finalize()[1:]  # Remove multibase char
        input_file.seek(0)

        now = datetime.datetime.now(tz=pytz.UTC)
        now_epoch_ms = int(now.timestamp() * 1000.0)

        transaction: DataCollectorTransaction = {
            "feed_id": self.feed_id,
            "data_id": data_digest,
            "save_date": now_epoch_ms,
            "data_fuuid": "",
            "key_ids": [],
            "pub_date_start": None,
            "pub_date_end": None,
            "attached_fuuids": None
        }

        return transaction


    async def _generate_output_content(self, transaction: DataCollectorTransaction, input_file: tempfile.TemporaryFile,
                                       output_file: tempfile.TemporaryFile, attached_files: Optional[list[AttachedFile]] = None) -> (str, int):

        # Encrypt the input data
        input_file_bytes: Optional[bytes] = await asyncio.to_thread(input_file.read)
        input_file.seek(0)

        cipher, cipher_info = await asyncio.to_thread(chiffrer_mgs4_bytes_secrete, self._encryption_key.secret_key, input_file_bytes)
        del input_file_bytes  # Release memory
        cipher_info['cle_id'] = self._encryption_key.key_id

        data_feed_file: DataFeedFile = {
            "feed_id": self.feed_id,
            "data_id": transaction["data_id"],
            "save_date": transaction.get('save_date'),
            "encrypted_data": cipher_info,
            "pub_start_date": transaction.get('pub_date_start'),
            "pub_end_date": transaction.get('pub_date_start') or transaction.get('save_date'),
            "files": attached_files
        }

        # Add information to transaction
        transaction['key_ids'].append(self._encryption_key.key_id)

        # Prepare output bytes, compress and produce fuuid
        output_file_bytes = json.dumps(data_feed_file).encode('utf-8')
        output_file_bytes = await asyncio.to_thread(zlib.compress, output_file_bytes)
        fuuid = await asyncio.to_thread(hacher, output_file_bytes, 'blake2b-512', 'base58btc')
        transaction['data_fuuid'] = fuuid

        # Save to output file
        await asyncio.to_thread(output_file.write, output_file_bytes)

        return fuuid, len(output_file_bytes)
