import os
import sys
import json
import shutil
import hashlib
import functools
import subprocess
import multiprocessing.dummy

import requests

INDEX_GIT_URL = 'https://github.com/rust-lang/crates.io-index.git'
CRATE_URL_FORMAT = 'https://static.crates.io/crates/{package_name}/{package_name}-{version}.crate'

HASH_BLOCK_SIZE = 4096

class Downloader:
    def __init__(self, index_path, mirror_path):
        self.index_path = index_path
        self.mirror_path = mirror_path

    def update_index(self):
        if os.path.isdir(self.index_path):
            print('Pulling latest changes from the index.')
            subprocess.check_output(['git', '-C', self.index_path, 'pull'])
        else:
            print('Downloading index.')
            subprocess.check_output(['git', 'clone', INDEX_GIT_URL, self.index_path])

    def get_packages(self):
        """Returns a list of `(package_names, path_in_index)`."""
        for (dirpath, dirname, filenames) in sorted(os.walk(self.index_path)):
            if dirpath == self.index_path:
                # No package at the root.
                continue
            elif any(x.startswith('.') for x in dirpath.split(os.path.sep)):
                # dotfile
                continue
            for filename in filenames:
                yield (filename, os.path.join(dirpath, filename))

    def get_releases(self, index_filename):
        """Returns the json dict of each release in the provided filename."""
        with open(index_filename) as fd:
            for line in fd:
                try:
                    yield json.loads(line)
                except:
                    print(repr(line))
                    raise

    def checksum(self, filename):
        sha = hashlib.sha256()
        with open(filename, 'rb') as fd:
            while True:
                data = fd.read(HASH_BLOCK_SIZE)
                if not data:
                    break
                sha.update(data)
        return sha.hexdigest()

    def is_already_downloaded(self, release):
        package_name = release['name']
        version = release['vers']
        target_dir = os.path.join(self.mirror_path, 'crates', package_name)
        target_filename = os.path.join(target_dir, '{}-{}.crate'.format(package_name, version))
        return os.path.isfile(target_filename)

    def download_release(self, release):
        package_name = release['name']
        version = release['vers']
        expected_checksum = release['cksum']
        target_dir = os.path.join(self.mirror_path, 'crates', package_name)
        target_filename = os.path.join(target_dir, '{}-{}.crate'.format(package_name, version))
        assert not os.path.isfile(target_filename)
        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)
        url = CRATE_URL_FORMAT.format(package_name=package_name, version=version)

        for x in range(100):
            try: response = requests.get(url, stream=True)
            except: continue
            else: break


        if response.status_code != 200:
            print('Could not download {}: HTTP code {}'.format(url, response.status_code))
            return
        
        try:
            with open(target_filename, 'ab') as fd:
                shutil.copyfileobj(response.raw, fd)

            actual_checksum = self.checksum(target_filename)
            if actual_checksum != expected_checksum:
                print('Checksum failed for {}: expected {}, got {}'.format(
                    target_filename, expected_checksum, actual_checksum))
                os.unlink(target_filename)
        except:
            os.unlink(target_filename)
            raise


    def download_package(self, args):
        (package_name, index_filename) = args
        print('Downloading {}'.format(package_name))
        try:
            for release in self.get_releases(index_filename):
                assert package_name.lower() == release['name'].lower()
                if not self.is_already_downloaded(release):
                    self.download_release(release)
        except:
            print('Failure while downloading {} ({}):'.format(package_name, index_filename))
            raise

def main():
    if len(sys.argv) == 3:
        (_, index_path, mirror_path) = sys.argv
        processes = 20
    elif len(sys.argv) == 4:
        (_, index_path, mirror_path, processes) = sys.argv
        processes = int(processes)
    else:
        print('Syntax: {} <index_path> <mirror_path> [<concurrency>]'.format(sys.argv[0]))
        exit(1)
    downloader = Downloader(index_path, mirror_path)
    downloader.update_index()
    with multiprocessing.dummy.Pool(processes) as pool:
        for x in pool.imap_unordered(downloader.download_package, downloader.get_packages()):
            pass

if __name__ == '__main__':
    main()
