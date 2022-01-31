import tarfile


class Compressor:
    def __init__(self, output_file_path):
        self.output_file_path = output_file_path
        self.file_paths = []

    def add(self, file_path):
        self.file_paths.append(file_path)

    def close(self):
        with tarfile.open(self.output_file_path, "w:gz") as _tar:
            for _file_path in self.file_paths:
                _tar.add(_file_path, arcname=_file_path)
