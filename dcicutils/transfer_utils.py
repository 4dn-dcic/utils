import os
import subprocess
import concurrent.futures
from env_utils import is_cgap_env
from creds_utils import CGAPKeyManager, SMaHTKeyManager
from ff_utils import search_metadata, get_download_url
from misc_utils import PRINT


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
            try:
                download_url = get_download_url(file_item['@id'])
            except Exception as e:
                PRINT(f'Could not retrieve download link for {filename} - is it a file type?')
                mapping[filename] = e
                continue
            if '.s3.amazonaws.com' not in download_url:
                PRINT(f'Potentially bad URL retrieved back from application: {download_url} - continuing')
            mapping[filename] = download_url
        return mapping

    def patch_location_to_portal(self, atid, file_path):
        """ Patches a special field to atid indicating it is redundantly stored at file_path """
        pass  # implement me after data model is in

    @staticmethod
    def download_curl(url: str, filename: str) -> str:
        """ Downloads from url under filename at the download path using curl """
        subprocess.run(['curl', '-L', url, '-o', filename], check=True)
        return filename

    @staticmethod
    def download_wget(url: str, filename: str) -> str:
        """ Downloads from url under filename at the download path using wget """
        subprocess.run(['wget', '-q', url, '-O', filename], check=True)
        return filename

    @staticmethod
    def download_rclone(url: str, filename: str) -> str:
        """ Downloads from url under filename at the download path using rclone """
        subprocess.run(['rclone', 'copy', url, filename], check=True)
        return filename

    @staticmethod
    def download_globus(url: str, filename: str) -> str:
        """ Downloads from url under filename at the download path using curl """
        subprocess.run(['globus', 'transfer', 'download', url, filename], check=True)
        return filename

    def download_file(self, url: str, filename: str) -> str:
        """ Entrypoint for general download, will select appropriate downloader depending on what was
            passed to init
        """
        filename = os.path.join(self.download_path, filename)
        if self.downloader == Downloader.CURL:
            return self.download_curl(url, filename)
        elif self.downloader == Downloader.WGET:
            return self.download_wget(url, filename)
        elif self.downloader == Downloader.GLOBUS:
            return self.download_globus(url, filename)
        else:  # rclone
            return self.download_rclone(url, filename)

    def parallel_download(self, filename_to_url_mapping: dict) -> list:
        """ Executes a parallel download given the result of extract_file_download_urls_from_search """
        download_files = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            for filename, download_url in filename_to_url_mapping.items():
                results = list(executor.map(self.download_file, download_url, filename))

        for result in results:
            if result is not None:
                download_files.append(result)
        return download_files
