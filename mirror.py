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
    def __init__(self, index_path, mirror_path, root_url):
        self.index_path = index_path
        self.mirror_path = mirror_path
        self.root_url = root_url

    def update_index(self):
        if os.path.isdir(self.index_path):
            print('Pulling latest changes from the index.')
            subprocess.check_output(['git', '-C', self.index_path, 'fetch', '--all'])
            subprocess.check_output(['git', '-C', self.index_path, 'reset', '--hard', 'origin/master'])
            os.unlink(os.path.join(self.index_path, 'config.json'))
        else:
            print('Downloading index.')
            subprocess.check_output(['git', 'clone', INDEX_GIT_URL, self.index_path])
        with open(os.path.join(self.index_path, 'config.json'), 'a') as fd:
            config = {'dl': self.root_url + '/api/v1/crates', 'api': self.root_url}
            fd.write(json.dumps(config))
        subprocess.check_output(['git', '-C', self.index_path, 'config', 'user.name', 'mirror'])
        subprocess.check_output(['git', '-C', self.index_path, 'commit', 'config.json', '-m', 'changing the API URL.'])

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
            except Exception:
                return {}

    def get_readme(self, tf, dir_in_crate, parsed_toml):
        readme_path = parsed_toml.get('package', {}).get('readme', None)
        if readme_path:
            if not isinstance(readme_path, str):
                return '"readme" key in Cargo.toml, but invalid value type'
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
        target_api_dir = os.path.join(self.mirror_path, 'api', 'v1', 'crates', package_name)
        crate_filename = os.path.join(target_dir, '{}-{}.crate'.format(package_name, version))
        dir_in_crate = '{}-{}'.format(package_name, version)
        if not os.path.isdir(os.path.join(target_dir, version)):
            os.makedirs(os.path.join(target_dir, version))
        if not os.path.isdir(os.path.join(target_api_dir, version)):
            os.makedirs(os.path.join(target_api_dir, version))
        try:
            os.symlink(os.path.abspath(crate_filename), os.path.join(target_api_dir, version, 'download'))
        except FileExistsError:
            pass

        html_filename = os.path.join(target_dir, version, 'index.html')
        html_timestamp = file_timestamp(html_filename)
        regen_html = html_timestamp < file_timestamp(crate_filename) or \
                html_timestamp < THIS_FILE_TIMESTAMP
        json_filename = os.path.join(target_api_dir, version, 'index.json')

        if regen_html or get_description:
            try:
                with tarfile.open(crate_filename, 'r') as tf:
                    try:
                        parsed_toml = self.parse_cargo_toml(dir_in_crate, tf)
                    except toml.TomlDecodeError:
                        return
                    if regen_html:
                        readme = self.get_readme(tf, dir_in_crate, parsed_toml)
                    license = parsed_toml.get('package', {}).get('license', None)
            except tarfile.ReadError:
                return
        else:
            license = '<skipped>' # sorry, I don't want to gunzip just to get the license
            parsed_toml = {}

        if regen_html:
            if os.path.exists(html_filename):
                os.unlink(html_filename)
            with open(html_filename, 'a') as fd:
                fd.write('<!DOCTYPE html><html><head><title>{name} version {vers}</title></head><body>\n'.format(**release))
                fd.write(markdown2.markdown(readme))
                try:
                    body = markdown2.markdown(readme)
                except UnicodeDecodeError as e:
                    body = '<h1>{name}</h1><p>Failed to generate description: {e}</p>'.format(**release, e=e)
                fd.write(body)
                fd.write('\n</body></html>')

        json_data = {'version': {
            'id': '{}-{}'.format(release['name'], release['vers']),
            'crate': release['name'],
            'num': release['vers'],
            'readme_path': '/api/v1/crates/{name}/{vers}/readme'.format(**release), # TODO
            'dl_path': '/api/v1/crates/{name}/{vers}/download'.format(**release), # TODO
            'features': release['features'],
            'yanked': release['yanked'],
            'license': license,
            'links': {}, # TODO
            }}
        if os.path.exists(json_filename):
            os.unlink(json_filename)
        with open(json_filename, 'a') as fd:
            json.dump(json_data, fd)

        return (parsed_toml.get('package', {}).get('description', 'No description'), json_data)


    def gen_package(self, package_name, index_filename):
        #print('Generating HTML for {}'.format(package_name))
        releases = list(self.get_releases(index_filename))
        try:
            releases.sort(key=lambda x: semver.parse_version_info(x['vers']))
        except ValueError:
            # eg. ValueError: 0.0.1-001 is not valid SemVer string
            pass
        description = ''
        json_versions_data = []
        for (i, release) in enumerate(releases):
            assert package_name.lower() == release['name'].lower()
            if not self.is_already_downloaded(release):
                continue
            ret = self.gen_release(release,
                    get_description=(i==len(releases)-1))
            if ret is None:
                continue
            (description, json_data) = ret
            json_versions_data.append(json_data)

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

        target_api_dir = os.path.join(self.mirror_path, 'api', 'v1', 'crates', package_name)
        if not os.path.isdir(target_api_dir):
            os.makedirs(target_api_dir)
        json_filename = os.path.join(target_api_dir, 'index.json')
        json_data = {
            'crate': {
                'id': package_name,
                'name': package_name,
                'versions': ['{name}-{vers}'.format(**release) for release in releases],
                'max_version': '{name}-{vers}'.format(**releases[-1]),
                'description': description,
                },
            'versions': json_versions_data,
            }
        if os.path.exists(json_filename):
            os.unlink(json_filename)
        with open(json_filename, 'a') as fd:
            json.dump(json_data, fd)
        print('wrote {}'.format(json_filename))

        return description


    def worker(self, args):
        (package_name, index_filename) = args
        self.download_package(package_name, index_filename)
        try:
            description = self.gen_package(package_name, index_filename)
        except:
            print("Error handling package {}".format(package_name))
            raise
        return (package_name, description)

def main():
    if len(sys.argv) == 4:
        (_, index_path, mirror_path, root_url) = sys.argv
        processes = 20
    elif len(sys.argv) == 5:
        (_, index_path, mirror_path, root_url, processes) = sys.argv
        processes = int(processes)
    else:
        print('Syntax: {} <index_path> <mirror_path> <root_url> [<concurrency>]'.format(sys.argv[0]))
        exit(1)
    downloader = Downloader(index_path, mirror_path, root_url)
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
