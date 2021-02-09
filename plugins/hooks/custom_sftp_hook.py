import pysftp
from io import BytesIO
from airflow.hooks.base_hook import BaseHook


class CustomSftpHook(BaseHook):
        
    def __init__(self, sftp_conn_id):
        self.connection = self.get_connection(sftp_conn_id)
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        self.conn = pysftp.Connection(self.connection.host, username=self.connection.login, password=self.connection.password, cnopts=cnopts)


    def upload_file_to_sftp(self, file_name: str):
        try:
            self.conn.put(file_name)
        except FileNotFoundError:
            print("File: {} not found on SFTP Server".format(file_name))


    def download_file_from_sftp(self, file_name: str, local_file_path=None):
        try:
            if local_file_path:
                self.conn.get(file_name, local_file_path)
            else:
                self.conn.get(file_name)
        except Exception as e:
            print("{}: Exception Occurred while downloading: {}".format(file_name, e))

    def get_file_data_from_sftp(self, file_name: str):
        flo = BytesIO()
        self.conn.getfo(file_name, flo)
        flo.seek(0)
        return flo

    def get_dir_list(self, dir_name: str) -> list:
        return self.conn.listdir(dir_name)
