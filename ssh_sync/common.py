import subprocess

def mkdirp(dirpath):
    subprocess.check_call('mkdir -p {}'.format(dirpath), shell=True)

