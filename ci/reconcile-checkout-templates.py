"""Reconcile genuine initialized inactive samples before guarded shell entry.

Both Git constructors execute a real init probe. Unknown active hooks refuse;
source, index, HEAD, and effective configuration remain unchanged. Retained
samples preserve their original inode, mode, and body. Configuration is held
only in this process and never hashed or written to the public receipt.
"""
from pathlib import Path
import argparse, hashlib, json, os, stat, subprocess, tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--root', required=True, type=Path)
parser.add_argument('--initialized-git', required=True, type=Path)
parser.add_argument('--selected-git', required=True, type=Path)
parser.add_argument('--receipt', required=True, type=Path)
parser.add_argument('--expected-head', required=True)
args = parser.parse_args()

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def regular(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode):
        raise RuntimeError('Expected an ordinary regular file')
    return {'device': info.st_dev, 'inode': info.st_ino,
            'mode': stat.S_IMODE(info.st_mode), 'sha256': digest(path)}

def ordinary_directory(path):
    if not stat.S_ISDIR(path.lstat().st_mode):
        raise RuntimeError('Expected an ordinary directory')

root = args.root.absolute()
for part in [root, *root.parents]:
    if part.is_symlink():
        raise RuntimeError('Symlink in owning checkout authority')
ordinary_directory(root)
git = args.selected_git.resolve(strict=True)
old_git = args.initialized_git.resolve(strict=True)
if not str(git).startswith('/nix/store/'):
    raise RuntimeError('Selected Git is not the declared immutable constructor')
images = {str(path): regular(path) for path in {git, old_git}}

def template_tree(directory):
    ordinary_directory(directory)
    rows = {}
    for parent, directories, files in os.walk(directory, followlinks=False):
        for name in ['.', *directories, *files]:
            path = Path(parent) if name == '.' else Path(parent) / name
            relative = path.relative_to(directory).as_posix()
            info = path.lstat()
            if stat.S_ISDIR(info.st_mode):
                row = ['directory', stat.S_IMODE(info.st_mode)]
            elif stat.S_ISREG(info.st_mode):
                row = ['file', stat.S_IMODE(info.st_mode), digest(path)]
            elif stat.S_ISLNK(info.st_mode):
                raise RuntimeError('Template supplier link refused')
            else:
                raise RuntimeError('Template supplier special member refused')
            rows[relative] = row
    return rows

def run(executable, *argv):
    return subprocess.check_output([str(executable), *map(str, argv)], cwd=root)

for variable in ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_TEMPLATE_DIR',
                  'GIT_CONFIG_PARAMETERS']:
    if os.environ.get(variable):
        raise RuntimeError('Inherited Git authority refused')
# Setup installs exactly one owner-scoped authentication entry. Keep its value
# only in task memory; path selectors and additional configuration stay refused.
auth_keys = {name: value for name, value in os.environ.items()
            if name == 'GIT_CONFIG_COUNT' or name.startswith('GIT_CONFIG_KEY_')
            or name.startswith('GIT_CONFIG_VALUE_')}
if auth_keys:
    if (set(auth_keys) != {'GIT_CONFIG_COUNT', 'GIT_CONFIG_KEY_0', 'GIT_CONFIG_VALUE_0'}
            or auth_keys['GIT_CONFIG_COUNT'] != '1'
            or auth_keys['GIT_CONFIG_KEY_0'] != 'http.https://github.com/metacraft-labs/.extraHeader'
            or not auth_keys['GIT_CONFIG_VALUE_0']):
        raise RuntimeError('Inherited Git authentication shape refused')
for key in ['core.hooksPath', 'init.templateDir']:
    result = subprocess.run([str(git), 'config', '--get', key], cwd=root,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode != 1:
        raise RuntimeError('Configured hook/template selector refused')
common = Path(run(git, 'rev-parse', '--path-format=absolute', '--git-common-dir').decode().strip())
if common != root / '.git':
    raise RuntimeError('Foreign common Git directory')
ordinary_directory(common)
hooks = common / 'hooks'
ordinary_directory(hooks)
head = run(git, 'rev-parse', 'HEAD').decode().strip()
if head != args.expected_head:
    raise RuntimeError('Checkout differs from declared event source')
index = regular(common / 'index')
config = run(git, 'config', '--null', '--show-origin', '--show-scope', '--list')

def source():
    rows = {}
    for entry in run(git, 'ls-files', '--stage', '-z').split(b'\0'):
        if not entry:
            continue
        metadata, name = entry.split(b'\t', 1)
        mode, blob, stage = metadata.split()
        if stage != b'0':
            raise RuntimeError('Unmerged source index')
        path = root / name.decode()
        if mode == b'120000':
            if not path.is_symlink():
                raise RuntimeError('Changed source symlink kind')
            body = os.readlink(path).encode()
        elif mode in {b'100644', b'100755'}:
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or bool(info.st_mode & 0o111) != (mode == b'100755'):
                raise RuntimeError('Changed source kind/mode')
            body = path.read_bytes()
        else:
            raise RuntimeError('Unsupported tracked source kind')
        if body != run(git, 'cat-file', 'blob', blob.decode()):
            raise RuntimeError('Tracked physical source differs from Git')
        rows[name.decode()] = [mode.decode(), hashlib.sha256(body).hexdigest()]
    return rows

def samples(directory):
    ordinary_directory(directory)
    rows = {}
    for path in sorted(directory.iterdir()):
        if not path.name.endswith('.sample'):
            raise RuntimeError('Unknown active hook refused')
        rows[path.name] = regular(path)
    if not rows:
        raise RuntimeError('Empty template inventory refused')
    return rows

def payload(rows):
    return {name: [row['mode'], row['sha256']] for name, row in rows.items()}

hook_info = hooks.lstat()
hooks_identity = (hook_info.st_dev, hook_info.st_ino, stat.S_IMODE(hook_info.st_mode))
def hooks_unchanged():
    info = hooks.lstat()
    return stat.S_ISDIR(info.st_mode) and (info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode)) == hooks_identity

constructors = {}
for executable in [old_git, git]:
    exec_path = Path(run(executable, '--exec-path').decode().strip())
    templates = exec_path.parent.parent / 'share/git-core/templates'
    constructors[str(executable)] = {'templates': str(templates), 'tree': template_tree(templates)}
before = samples(hooks)
source_before = source()
receipt = {'scope': __doc__, 'head': head, 'finished': False, 'success': False,
            'images': images, 'constructors': constructors, 'initializedBefore': before}
args.receipt.parent.mkdir(parents=True, exist_ok=True)
if args.receipt.exists() or args.receipt.is_symlink():
    raise RuntimeError('Receipt already exists')
retained = None

def directory_identity(path):
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode): raise RuntimeError('Directory authority changed')
    return (info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode))
root_identity = directory_identity(root)
common_identity = directory_identity(common)
def parents_unchanged():
    return directory_identity(root) == root_identity and directory_identity(common) == common_identity

def worktree_inventory():
    rows = {}
    for parent, directories, files in os.walk(root, followlinks=False):
        if Path(parent) == root: directories[:] = [name for name in directories if name != '.git']
        for name in ['.', *directories, *files]:
            path = Path(parent) if name == '.' else Path(parent)/name
            if path == args.receipt.absolute(): continue
            info = path.lstat(); relative = path.relative_to(root).as_posix()
            if stat.S_ISDIR(info.st_mode): row = ['D', stat.S_IMODE(info.st_mode)]
            elif stat.S_ISREG(info.st_mode): row = ['F', stat.S_IMODE(info.st_mode), digest(path)]
            elif stat.S_ISLNK(info.st_mode): row = ['L', stat.S_IMODE(info.st_mode), os.readlink(path)]
            else: raise RuntimeError('Worktree special member refused')
            rows[relative] = row
    return rows
worktree_before = worktree_inventory()

def save():
    args.receipt.write_text(json.dumps(receipt, indent=2) + '\n')

save()
try:
    initialized = []
    for executable in [old_git, git]:
        with tempfile.TemporaryDirectory(prefix='trace-git-template-probe-') as directory:
            probe = Path(directory) / 'probe'
            subprocess.run([str(executable), 'init', str(probe)], cwd=root, check=True,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            probe_payload = payload(samples(probe / '.git/hooks'))
            template_payload = payload(samples(Path(constructors[str(executable)]['templates']) / 'hooks'))
            # Genuine Git init copies immutable templates to owner-writable samples.
            # Keep exact source modes in constructors; compare the observed copy mode.
            template_payload = {name: [mode | stat.S_IWUSR, sha] for name, (mode, sha) in template_payload.items()}
            if probe_payload != template_payload:
                raise RuntimeError('Actual init differs from selected template supplier')
            initialized.append(probe_payload)
    if payload(before) != initialized[0]:
        raise RuntimeError('Initialized samples differ from their actual constructor')
    if not hooks_unchanged() or samples(hooks) != before:
        raise RuntimeError('Hook identity changed before retention')
    retained = Path(tempfile.mkdtemp(prefix='trace-retained-inactive-samples-', dir=common))
    if not parents_unchanged(): raise RuntimeError('Owning directory changed before retention')
    hooks_fd = os.open(hooks, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    retained_fd = os.open(retained, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    retained_identity = directory_identity(retained)
    def held_parents():
        for fd, expected in [(hooks_fd, hooks_identity), (retained_fd, retained_identity)]:
            info = os.fstat(fd)
            if not stat.S_ISDIR(info.st_mode) or (info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode)) != expected:
                raise RuntimeError('Held directory identity differs')
        if not parents_unchanged() or directory_identity(retained) != retained_identity or not hooks_unchanged():
            raise RuntimeError('Retention pathname authority changed')
    try:
        held_parents()
        for name in before:
            held_parents()
            if regular(hooks / name) != before[name]:
                raise RuntimeError('Sample changed before held-parent rename')
            os.rename(name, name, src_dir_fd=hooks_fd, dst_dir_fd=retained_fd)
    finally:
        os.close(retained_fd)
        os.close(hooks_fd)
    if samples(retained) != before:
        raise RuntimeError('Retained sample identity/body changed')
    subprocess.run([str(git), 'init', str(root)], cwd=root, check=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not hooks_unchanged() or payload(samples(hooks)) != initialized[1]:
        raise RuntimeError('Selected constructor did not initialize its genuine templates')
    receipt['success'] = True
finally:
    guards = {
        'sourceUnchanged': lambda: source() == source_before,
        'worktreeUnchanged': lambda: worktree_inventory() == worktree_before,
        'indexUnchanged': lambda: regular(common / 'index') == index,
        'headUnchanged': lambda: run(git, 'rev-parse', 'HEAD').decode().strip() == head,
        'authenticationEnvironmentUnchanged': lambda: {name: value for name, value in os.environ.items() if name == 'GIT_CONFIG_COUNT' or name.startswith('GIT_CONFIG_KEY_') or name.startswith('GIT_CONFIG_VALUE_')} == auth_keys,
        'configUnchanged': lambda: run(git, 'config', '--null', '--show-origin', '--show-scope', '--list') == config,
        'hookDirectoryUnchanged': hooks_unchanged,
        'owningDirectoriesUnchanged': parents_unchanged,
        'constructorsUnchanged': lambda: all(template_tree(Path(row['templates'])) == row['tree'] for row in constructors.values()),
        'imagesUnchanged': lambda: all(regular(Path(name)) == row for name, row in images.items()),
        'retainedUnchanged': lambda: retained is None or (directory_identity(retained) == retained_identity and samples(retained) == before),
    }
    for name, check in guards.items():
        try: receipt[name] = bool(check())
        except Exception as error:
            receipt[name] = False
            receipt.setdefault('guardErrors', {})[name] = type(error).__name__
    receipt['retainedDirectory'] = str(retained) if retained else None
    receipt['finished'] = True
    receipt['success'] = receipt['success'] and all(receipt[name] for name in guards)
    save()
if not receipt['success']:
    raise RuntimeError('Final template reconciliation guard refused')
