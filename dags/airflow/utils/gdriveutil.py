import json
import io
import logging
from datetime import datetime, timezone

import numpy as np
import pytz
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from httplib2 import Http

from airflow.hooks.base_hook import BaseHook

GOOGLE_SERVICE_ACCOUNT_CREDENTIALS = BaseHook.get_connection("demo_google_service_account")


class GDriveUtil:
    def __init__(self):
        self.scope = [
            "https://www.googleapis.com/auth/drive",
        ]
        self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            json.loads(
                json.loads(GOOGLE_SERVICE_ACCOUNT_CREDENTIALS.extra)[
                    "extra__google_cloud_platform__keyfile_dict"
                ]
            ),
            self.scope,
        )

        self.http_auth = self.credentials.authorize(Http())
        # print("self.http_auth:" + str(self.http_auth))
        # dir_ = dir(self.http_auth)
        # for att in dir_:
        #     attr = getattr(self.http_auth, att)
        #     print(f"att: {att} - {attr}")
        #     _dir_ = dir(attr)
        #     for _att in _dir_:
        #         _attr = getattr(attr, _att)
        #         print(f"_att: {_att} - {_attr}")
        self.access_token = self.credentials.get_access_token(Http()).access_token
        # print("self.access_token:" + str(access_token))
        # dir_ = dir(access_token)
        # for att in dir_:
        #     attr = getattr(access_token, att)
        #     print(f"att: {att} - {attr}")
        #     _dir_ = dir(attr)
        #     for _att in _dir_:
        #         _attr = getattr(attr, _att)
        #         print(f"_att: {_att} - {_attr}")
        self.drive = build("drive", "v3", http=self.http_auth, cache_discovery=False)

    def get_file_metadata(self, file_id=None, file_name=None):
        if file_name:
            res = self.drive.files().list().execute()
            print("res" + str(res))
            return [fi for fi in res["files"] if fi["name"] == file_name][0]
        else:
            return self.drive.files().get(fileId=file_id, supportsAllDrives=True).execute()
        # return self.drive.files().list().execute()

    def get_file_as_stream(self, file_id):
        request = self.drive.files().get_media(fileId=file_id, supportsAllDrives=True)
        f = io.BytesIO()
        media_request = googleapiclient.http.MediaIoBaseDownload(f, request)

        done = False
        while done is False:
            status, done = media_request.next_chunk()
            logging.info("Download %d%%." % int(status.progress() * 100))
        f.seek(0)
        return f

    def get_revision_id(self, file_id, time_of_revision, revision_list_tz=pytz.utc):
        res = self.drive.revisions().list(fileId=file_id,).execute()

        fmt_in = "%Y-%m-%dT%H:%M:%S.%fZ"
        fmt_out = "%Y-%m-%d %H:%M:00"

        for revision in res["revisions"]:
            ts = datetime.strptime(revision["modifiedTime"], fmt_in).replace(tzinfo=pytz.UTC)
            minute_trunc = datetime.strftime(ts.astimezone(revision_list_tz), fmt_out)
            print(
                "Revision:"
                + str(revision)
                + f": minute_trunc - {minute_trunc}: tor - {time_of_revision}"
            )

            if minute_trunc == time_of_revision:
                return revision["id"]

    def get_file_id(self, fi_name):
        res = self.drive.files().list().execute()
        return [fi["id"] for fi in res["files"] if fi["name"] == fi_name][0]

    def get_access_token(self):
        return self.access_token
