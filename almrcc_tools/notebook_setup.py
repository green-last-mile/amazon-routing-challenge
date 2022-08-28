from typing import Union
import os
import sys
from dotenv import load_dotenv
from io import StringIO, BytesIO
import plotly.graph_objects as go


# HACKY way to test if on remote cluster or not
LOCAL = "ext" not in os.getlogin()
print("Assuming local environment" if LOCAL else "Assuming remote environment")


# Meta Scripting, build same methods for all notebooks. Local and Remote
if not LOCAL:
    from google.cloud import storage

    class _Base:
        def __init__(self):
            self._client = storage.Client()
            self._load_dotenv()

        def _load_dotenv(self):
            load_dotenv(
                stream=StringIO(
                    self._get_bucket("kale-dataproc-notebook")
                    .blob("max/.env")  # TODO: don't hardcode this
                    .download_as_string()
                    .decode()
                )
            )

        @staticmethod
        def _get_bucket_n_blob(file_path: str) -> tuple:
            bucket, file_name = os.path.split(file_path[::-1])
            return bucket[::-1], file_name[::-1]

        def _get_bucket(self, bucket_name):
            return self._client.get_bucket(bucket_name)

        def get_file(self, file_path: str) -> bytes:
            bucket, file_name = self._get_bucket_n_blob(file_path)
            blob = self._get_bucket(bucket).blob(file_name)
            return blob.download_as_bytes()

        def _walk_folder(self, folder_path: str) -> list:
            bucket, folder_name = self._get_bucket_n_blob(folder_path)
            blobs = self._get_bucket(bucket).list_blobs(prefix=folder_name)
            return [blob.name for blob in blobs]

        def _write_file(self, file_path: str, content: bytes):
            bucket, file_name = self._get_bucket_n_blob(file_path)
            self._get_bucket(bucket).blob(file_name).upload_from_file(content, rewind=True)

else:
    import pathlib

    class _Base:
        def __init__(self):
            self._base_path = self._find_root(pathlib.Path().absolute())
            sys.path.append(self._base_path)
            self._load_dotenv()

        def _load_dotenv(self):
            load_dotenv(os.path.join(self._base_path, ".env"))

        @staticmethod
        def _find_root(path):
            if os.path.split(path)[-1] != "amazon-routing-challenge":
                return _Base.find_root(os.path.split(path)[0])
            return path

        def get_file(self, file_path: str) -> bytes:
            with open(file_path, "rb") as f:
                return f.read()

        def _walk_folder(self, folder_path: str) -> list:
            return [os.path.join(folder_path, file) for file in os.listdir(folder_path)]

        def _write_file(self, file_path: str, content: BytesIO):
            with open(file_path, "wb") as f:
                f.write(content.getbuffer())


class _FileHandler(_Base):
    def __init__(
        self,
    ):
        super().__init__()

    def get_file(self, file_path: str) -> bytes:
        return super().get_file(file_path)

    def get_file_stream(self, file_path) -> BytesIO:
        return BytesIO(self.get_file(file_path))

    def walk_folder(self, folder_path: str) -> list:
        return self._walk_folder(folder_path)

    def write_file(self, file_path: str, obj: Union[bytes, str]) -> None:
        if isinstance(obj, str):
            obj = obj.encode()
        buffer = BytesIO() 
        buffer.write(obj)
        self._write_file(file_path, buffer)


GLMFileHandler = _FileHandler()


class MapboxPlot(go.Figure):
    def __init__(self, sat_background=True, **kwargs):
        super().__init__(
            layout=go.Layout(
                mapbox=go.layout.Mapbox(
                    accesstoken=os.environ.get("MAPBOX_KEY"),
                    style="mapbox://styles/max-schrader/cl6lhvrfw001516pkh3s6iv7l"
                    if sat_background
                    else "mapbox://styles/max-schrader/ck8t1cmmc02wk1it9rv28iyte",
                    bearing=0,
                    zoom=10,
                )
            )
        )

    def set_center(self, lat, lon) -> None:
        self.layout.mapbox.center = go.layout.mapbox.Center(lat=lat, lon=lon)

    def set_zoom(self, zoom) -> None:
        self.layout.mapbox.zoom = zoom


from openrouteservice.client import Client


class ORSClient(Client):
    def __init__(
        self,
        base_url=...,
        timeout=60,
        retry_timeout=60,
        requests_kwargs=None,
        retry_over_query_limit=True,
    ):
        super().__init__(
            os.environ["ORS_KEY"],
            base_url,
            timeout,
            retry_timeout,
            requests_kwargs,
            retry_over_query_limit,
        )
