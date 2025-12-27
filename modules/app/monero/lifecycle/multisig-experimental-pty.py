import os, pty, subprocess, select, time, sys

cli = os.environ.get('ZNODE_MWCLI_PATH') or 'monero-wallet-cli'
wallet_file = os.environ.get('ZNODE_MWCLI_WALLET_FILE') or ''
password_file = os.environ.get('ZNODE_MWCLI_PASSWORD_FILE') or ''
daemon_address = os.environ.get('ZNODE_MWCLI_DAEMON_ADDRESS') or ''
daemon_login = os.environ.get('ZNODE_MWCLI_DAEMON_LOGIN') or ''
try:
  timeout = float(os.environ.get('ZNODE_MWCLI_TIMEOUT_SEC') or '30')
except Exception:
  timeout = 30.0

pw = ''
try:
  with open(password_file, 'r', encoding='utf-8', errors='ignore') as f:
    for line in f:
      pw = line.rstrip()
      break
except Exception:
  pw = ''

cmd = [cli, '--wallet-file', wallet_file, '--password-file', password_file]
if daemon_address:
  cmd += ['--daemon-address', daemon_address]
if daemon_login:
  cmd += ['--daemon-login', daemon_login]
cmd += ['--offline']

master, slave = pty.openpty()
p = subprocess.Popen(cmd, stdin=slave, stdout=slave, stderr=slave, close_fds=True)
os.close(slave)

text = ''
invalid_pw = False
answered_bg = False
stage = 0
sent_set_at = None
deadline = time.time() + timeout

while True:
  if time.time() > deadline:
    break
  r, _, _ = select.select([master], [], [], 0.2)
  if r:
    try:
      chunk = os.read(master, 4096)
    except OSError:
      chunk = b''
    if chunk:
      try:
        sys.stdout.buffer.write(chunk)
        sys.stdout.flush()
      except Exception:
        pass
      if b'Error: invalid password' in chunk:
        invalid_pw = True
      try:
        s = chunk.decode(errors='ignore')
      except Exception:
        s = ''
      text += s
      if len(text) > 200000:
        text = text[-200000:]
    else:
      break

  if (not answered_bg) and ('Do you want to do it now? (Y/Yes/N/No):' in text):
    try:
      os.write(master, b'N\n')
    except Exception:
      pass
    answered_bg = True

  if stage == 0 and ('[wallet' in text and ']: ' in text):
    try:
      os.write(master, b'set enable-multisig-experimental 1\n')
    except Exception:
      pass
    sent_set_at = time.time()
    stage = 1

  if stage == 1 and ('Wallet password:' in text):
    try:
      os.write(master, (pw + '\n').encode())
    except Exception:
      pass
    stage = 2

  if stage in (1, 2) and ('[wallet' in text and ']: ' in text):
    if stage == 1 and sent_set_at and (time.time() - sent_set_at) > 2.0:
      try:
        os.write(master, b'exit\n')
      except Exception:
        pass
      stage = 3
    elif stage == 2:
      try:
        os.write(master, b'exit\n')
      except Exception:
        pass
      stage = 3

  if p.poll() is not None:
    break

if p.poll() is None:
  try:
    p.terminate()
  except Exception:
    pass
  try:
    p.wait(timeout=2)
  except Exception:
    try:
      p.kill()
    except Exception:
      pass
    try:
      p.wait(timeout=2)
    except Exception:
      pass

try:
  os.close(master)
except Exception:
  pass

if p.returncode not in (0, None):
  sys.exit(p.returncode)
if stage == 0:
  sys.exit(2)
if invalid_pw:
  sys.exit(3)
sys.exit(0)
