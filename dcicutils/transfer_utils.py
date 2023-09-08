import os
from ff_utils import search_metadata
import subprocess
import concurrent.futures
from env_utils import is_cgap_env
from creds_utils import CGAPKeyManager, SMaHTKeyManager


class TransferUtilsError(Exception):
    pass


class Downloader:
    CURL = 'curl'
    WGET = 'wget'
    RCLONE = 'rclone'
    GLOBUS = 'globus'
    VALID_DOWNLOADERS = [
        CURL, WGET, RCLONE, GLOBUS
    ]


class TransferUtils:
    """ Utility class for downloading files to a local system """

    def __init__(self, *, ff_env, num_processes=8, download_path, downloader=Downloader.CURL):
        """ Builds the TransferUtils object, initializing Auth etc """
        self.num_processes = num_processes
        self.download_path = download_path
        if downloader not in Downloader.VALID_DOWNLOADERS:
            raise TransferUtilsError(f'Passed invalid/unsupported downloader to TransferUtils: {downloader}')
        self.downloader = downloader.lower()
        self.key = (CGAPKeyManager().get_keydict_for_env(ff_env) if is_cgap_env else
                    SMaHTKeyManager().get_keydict_for_env(ff_env))

    def initialize_download_path(self):
        """ Creates dirs down to the path if they do not exist """
        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

    def extract_file_download_urls_from_search(self, search: str) -> dict:
        """ Returns dictionary mapping file names to URLs from a File search """
        mapping = {}
        for file_item in search_metadata(search, key=self.key):
            filename = file_item['accession']
            download_url = f'{self.key.get("server", "invalid-keyfile")}/{file_item["@id"]}/@@download'
            mapping[filename] = download_url
        return mapping

    @staticmethod
    def download_curl(url: str, filename: str) -> None:
        """ Downloads from url under filename at the download path using curl """
        subprocess.run(['curl', '-L', url, '-o', filename], check=True)

    @staticmethod
    def download_wget(url: str, filename):
        """ Downloads from url under filename at the download path using wget """
        subprocess.run(['wget', '-q', url, '-O', filename], check=True)

    @staticmethod
    def download_rclone(url, filename):
        """ Downloads from url under filename at the download path using rclone """
        subprocess.run(['rclone', 'copy', url, filename], check=True)

    @staticmethod
    def download_globus(url, filename):
        """ Downloads from url under filename at the download path using curl """
        subprocess.run(['globus', 'transfer', 'download', url, filename], check=True)

    def download_file(self, url, filename):
        """ Entrypoint for general download, will select appropriate downloader depending on what was
            passed to init
        """
        if self.downloader == Downloader.CURL:
            return self.download_curl(url, filename)
        elif self.downloader == Downloader.WGET:
            return self.download_wget(url, filename)
        elif self.downloader == Downloader.GLOBUS:
            return self.download_globus(url, filename)
        else:  # rclone
            return self.download_rclone(url, filename)

    def parallel_download(self, filename_to_url_mapping):
        """ Executes a parallel download given the result of extract_file_download_urls_from_search """
        download_files = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            for filename, download_url in filename_to_url_mapping.items():
                results = list(executor.map(self.download_file, download_url, filename))

        for result in results:
            if result is not None:
                download_files.append(result)
        return download_files
