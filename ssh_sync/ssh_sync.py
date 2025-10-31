
import os, time
import struct
import logging
import gzip
import io
import paramiko
import typer
from ssh_sync.common import mkdirp

logging.basicConfig(
    format='%(asctime)s [%(levelname)-6s] [%(filename)s:%(lineno)d]  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO
)

app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    help='sync tool by ssh'
)

class SSHKeeper:
    def __init__(self, ssh_host: str, ssh_port: int = 22):
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(hostname=ssh_host, port=ssh_port, timeout=10)
        self.transport = self.ssh.get_transport()
        self.transport.set_keepalive(interval=30)
        logging.info('[connect] {ssh_host}:{ssh_port}'.format(**locals()))

    def execute_one(self, remote_cmd):
        stdin, stdout, stderr = self.ssh.exec_command(remote_cmd)
        compressed_data = stdout.read()
        error = stderr.read()
        if error:
            return None, error
        with io.BytesIO(compressed_data) as f_compressed:
            with gzip.GzipFile(fileobj=f_compressed, mode='rb') as gz:
                original_data = gz.read()
                return original_data, None


class MMapVecFetcher:
    def __init__(self, ssh_keeper, source_prefix, target_prefix,
            limit_mb=4.0,
            streaming=False,
            sleep_interval=1.0,
            ):
        self.ssh_keeper = ssh_keeper
        self.source_prefix = source_prefix
        self.target_prefix = target_prefix
        self.limit_mb = limit_mb
        self.streaming = streaming
        self.sleep_interval = sleep_interval
        # init
        self.source_data_file = self.source_prefix + '_data'
        self.source_meta_file = self.source_prefix + "_meta"
        self.target_data_file = self.target_prefix + '_data'
        self.target_meta_file = self.target_prefix + "_meta"
        self.struct_bytes = 0
        self.data_length = 0

    def fetch_remote_meta(self):
        cmd = 'od -An -tu8 {} | gzip -c --no-name -q'.format(self.source_meta_file)
        data, error = self.ssh_keeper.execute_one(cmd)
        if not error:
            struct_bytes, data_length = (int(c.strip()) for c in data.decode('utf-8').strip().split(' ', 1))
            if self.struct_bytes == 0:
                self.struct_bytes = struct_bytes
                self.limit_length = int(self.limit_mb * 1024 * 1024 / self.struct_bytes)
            else:
                assert self.struct_bytes == struct_bytes, 'struct bytes mismatch! {} vs {}'.format(self.struct_bytes, struct_bytes)
            assert data_length >= self.data_length, 'data length decrease! {} vs {}'.format(self.data_length, data_length)
            self.data_length = data_length
            logging.info('[META_INFO] [struct_bytes] {} [data_length] {} [local_data_length] {}'.format(self.struct_bytes, self.data_length, self.local_data_length))

    def dump_local_meta(self, target_length):
        self.local_data_length = target_length
        bytes_buf = struct.pack('qq', self.struct_bytes, target_length)
        self.meta_fd.seek(0)
        self.meta_fd.write(bytes_buf)
        self.meta_fd.flush()
        os.fsync(self.meta_fd.fileno())

    def fetch(self):
        if os.path.exists(self.target_meta_file):
            read_bytes = open(self.target_meta_file, 'rb').read()
            local_struct_bytes, self.local_data_length = struct.unpack('qq', read_bytes)
            self.data_fd = open(self.target_data_file, 'rb+')
            self.meta_fd = open(self.target_meta_file, 'rb+')
        else:
            self.local_data_length = 0
            self.data_fd = open(self.target_data_file, 'wb+')
            self.meta_fd = open(self.target_meta_file, 'wb+')
        self.fetch_remote_meta()
        assert self.struct_bytes > 0, 'struct_bytes({}) error!'.format(self.struct_bytes)
        while True:
            self.fetch_one()
            if not self.streaming:
                if self.data_length == self.local_data_length:
                    break
            if self.data_length == self.local_data_length:
                time.sleep(1)
            else:
                time.sleep(self.sleep_interval)
        logging.info('[Done]')

    def fetch_one(self):
        # update meta
        self.fetch_remote_meta()
        # do fetch
        assert self.data_length >= self.local_data_length, 'decrease length: {} vs {}'.format(self.local_data_length, self.data_length)
        if self.data_length == self.local_data_length:
            return
        local_bytes = self.local_data_length * self.struct_bytes
        target_length = min(self.data_length, self.local_data_length + self.limit_length)
        target_bytes = target_length * self.struct_bytes
        send_bytes = target_bytes - local_bytes
        cmd = 'dd if={self.source_data_file} skip={local_bytes} count={send_bytes} iflag=skip_bytes,count_bytes status=none | gzip -q --no-name -c'.format(**locals())
        data, error = self.ssh_keeper.execute_one(cmd)
        if not error:
            self.data_fd.seek(local_bytes)
            self.data_fd.write(data)
            self.data_fd.flush()
            os.fsync(self.data_fd.fileno())
            self.dump_local_meta(target_length)

@app.command()
def sync_vec(ssh_host: str, source_prefix: str, target_prefix: str,
        ssh_port:int = 22,
        limit_mb:float = 4.0,
        sleep_interval:float = 0.01,
        streaming: bool = False):
    sk = SSHKeeper(ssh_host, ssh_port=ssh_port)
    mmapvec_fetcher = MMapVecFetcher(sk, source_prefix, target_prefix, limit_mb=limit_mb, streaming=streaming, sleep_interval=sleep_interval)
    mmapvec_fetcher.fetch()

@app.command()
def dummy():
    pass

def cli():
    app()

if __name__ == '__main__':
    app()
    #import argh
    #argh.dispatch_commands([
    #    sync_vec
    #])
