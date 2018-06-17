import os
import sys
import json
import shutil
import tarfile
import hashlib
import functools
import subprocess
import multiprocessing

import toml
import semver
import requests
import markdown2

INDEX_GIT_URL = 'https://github.com/rust-lang/crates.io-index.git'
CRATE_URL_FORMAT = 'https://static.crates.io/crates/{package_name}/{package_name}-{version}.crate'

HASH_BLOCK_SIZE = 4096

def file_timestamp(filename):
    try:
        statbuf = os.stat(filename)
        return statbuf.st_mtime
    except FileNotFoundError:
        return 0

THIS_FILE_TIMESTAMP = file_timestamp(__file__)

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
                except Exception as e:
                    print(repr(line))
                    raise e

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


    def download_package(self, package_name, index_filename):
        #print('Downloading {}'.format(package_name))
        try:
            for release in self.get_releases(index_filename):
                assert package_name.lower() == release['name'].lower()
                if not self.is_already_downloaded(release):
                    self.download_release(release)
        except:
            print('Failure while downloading {} ({}):'.format(package_name, index_filename))
            raise



    def parse_cargo_toml(self, dir_in_crate, tf):
        try:
            ti = tf.getmember(os.path.join(dir_in_crate, 'Cargo.toml'))
            assert ti.isfile()
        except KeyError:
            return {}
        except EOFError:
            return {}
        with tf.extractfile(ti) as toml_file:
            try:
                return toml.loads(toml_file.read().decode())
            except toml.TomlDecodeError:
                return {}
            except Exception as e:
                raise Exception('Failed unTOMLing Cargo.toml from {}'.format(crate_filename))

    def get_readme(self, tf, dir_in_crate, parsed_toml):
        readme_path = parsed_toml.get('package', {}).get('readme', None)
        if readme_path:
            if readme_path.startswith('./'):
                readme_path = readme_path[2:]
            readme_path = os.path.join(dir_in_crate, readme_path)
            try:
                with tf.extractfile(readme_path) as readme_file:
                    return readme_file.read()
            except KeyError:
                return '"readme" key in Cargo.toml, but no file with that name.'
        else:
            try:
                for member in tf.getmembers():
                    if member.name.startswith(os.path.join(dir_in_crate, 'README')):
                        with tf.extractfile(member) as readme_file:
                            return readme_file.read()
            except EOFError:
                return 'EOF while scanning crate.'
            return 'No README found.'

    def gen_release(self, release, get_description):
        package_name = release['name']
        version = release['vers']
        target_dir = os.path.join(self.mirror_path, 'crates', package_name)
        crate_filename = os.path.join(target_dir, '{}-{}.crate'.format(package_name, version))
        dir_in_crate = '{}-{}'.format(package_name, version)
        if not os.path.isdir(os.path.join(target_dir, version)):
            os.makedirs(os.path.join(target_dir, version))

        html_filename = os.path.join(target_dir, version, 'index.html')
        html_timestamp = file_timestamp(html_filename)
        regen_html = html_timestamp < file_timestamp(crate_filename) or \
                html_timestamp < THIS_FILE_TIMESTAMP

        if not regen_html and not get_description:
            return

        try:
            with tarfile.open(crate_filename, 'r') as tf:
                parsed_toml = self.parse_cargo_toml(dir_in_crate, tf)
                if regen_html:
                    readme = self.get_readme(tf, dir_in_crate, parsed_toml)
        except tarfile.ReadError:
            return

        if regen_html:
            if os.path.exists(html_filename):
                os.unlink(html_filename)
            with open(html_filename, 'a') as fd:
                fd.write('<!DOCTYPE html><html><head><title>{name} version {vers}</title></head><body>\n'.format(**release))
                fd.write(markdown2.markdown(readme))
                fd.write('\n</body></html>')

        return parsed_toml.get('package', {}).get('description', 'No description')


    def gen_package(self, package_name, index_filename):
        #print('Generating HTML for {}'.format(package_name))
        releases = list(self.get_releases(index_filename))
        releases.sort(key=lambda x: semver.parse_version_info(x['vers']))
        description = ''
        for (i, release) in enumerate(releases):
            assert package_name.lower() == release['name'].lower()
            if not self.is_already_downloaded(release):
                continue
            description = self.gen_release(release,
                    get_description=(i==len(releases)-1))

        target_dir = os.path.join(self.mirror_path, 'crates', package_name)

        if not os.path.isdir(target_dir):
            os.makedirs(target_dir)

        html_filename = os.path.join(target_dir, 'index.html')
        if os.path.exists(html_filename):
            os.unlink(html_filename)
        with open(html_filename, 'a') as fd:
            fd.write('<!DOCTYPE html><html><head><title>{}</title></head><body><ul>\n'.format(package_name))
            for release in releases:
                fd.write('<li><a href="./{vers}/">{vers}</a></li>\n'.format(**release))
            fd.write('</ul></body></html>')

        return description


    def worker(self, args):
        (package_name, index_filename) = args
        self.download_package(package_name, index_filename)
        description = self.gen_package(package_name, index_filename)
        return (package_name, description)

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

    if not os.path.isdir(mirror_path):
        os.makedirs(mirror_path)
    html_filename = os.path.join(mirror_path, 'index.html')
    if os.path.exists(html_filename):
        os.unlink(html_filename)
    with open(html_filename, 'a') as fd:
        fd.write('<!DOCTYPE html><html><head><title>Crates</title></head><body><dl>\n')
        with multiprocessing.Pool(processes) as pool:
            for (name, description) in pool.imap_unordered(downloader.worker, downloader.get_packages()):
                """
        for (name, description) in map(downloader.worker, downloader.get_packages()):
                """
                fd.write('<dt><a href="crates/{name}/">{name}</a></dt>\n<dd>{description}</dd>\n\n'
                        .format(name=name, description=description))
        fd.write('</dl></body></html>')

if __name__ == '__main__':
    main()
