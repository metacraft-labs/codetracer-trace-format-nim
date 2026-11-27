"""Reconcile genuine initialized inactive samples before guarded shell entry.

Both Git constructors execute a real init probe. Unknown active hooks refuse;
source, index, HEAD, and effective configuration remain unchanged. Retained
samples preserve their original inode, mode, and body. Configuration is held
only in this process and never hashed or written to the public receipt.
"""
from pathlib import Path
import argparse, hashlib, json, os, re, stat, subprocess, sys, tempfile

def git_template_copy_mode(source_mode, process_umask):
    """Return the mode `git init` gives a regular file copied from a template.

    Git's copy_templates_1() passes the template's st_mode to copy_file(),
    which keeps only "is any execute bit set" and opens the destination with
    0777 or 0666; the kernel then applies the process umask:

        mode = (mode & 0111) ? 0777 : 0666;
        fdo = open(dst, O_WRONLY | O_CREAT | O_EXCL, mode);

    https://github.com/git/git/blob/v2.54.0/copy.c#L38-L48 (unchanged in
    v2.43.0 and v2.51.0; the caller is setup.c copy_templates_1()). The source
    mode's other bits never reach the copy, so a 0755 supplier initializes
    0775 under umask 0002 and 0755 under 0022. copy_file() then calls
    adjust_shared_perm(), a no-op unless core.sharedRepository is set; such a
    configuration makes the prediction (and so reconciliation) refuse rather
    than pass.
    """
    return (0o777 if source_mode & 0o111 else 0o666) & ~process_umask & 0o777

def self_test(executables):
    """Check git_template_copy_mode() against real `git init` copies.

    No mocks: each named Git initializes from a synthetic template directory
    under several umasks, and every observed sample mode must equal the
    prediction. Global/system configuration is excluded so an operator's
    core.sharedRepository cannot change what is measured.
    """
    env = {**os.environ, 'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1'}
    for variable in ['GIT_DIR', 'GIT_WORK_TREE', 'GIT_COMMON_DIR', 'GIT_TEMPLATE_DIR',
                     'GIT_CONFIG_PARAMETERS', 'GIT_CONFIG_COUNT']:
        env.pop(variable, None)
    source_modes = [0o755, 0o644, 0o555, 0o444, 0o700, 0o600, 0o711]
    checked = 0
    with tempfile.TemporaryDirectory(prefix='trace-git-template-self-test-') as directory:
        templates = Path(directory) / 'templates'
        (templates / 'hooks').mkdir(parents=True)
        for source_mode in source_modes:
            sample = templates / 'hooks' / f'{source_mode:04o}.sample'
            sample.write_text(f'{source_mode:04o}\n')
            sample.chmod(source_mode)
        for executable in executables:
            for process_umask in [0o002, 0o022, 0o027, 0o077]:
                probe = Path(directory) / f'probe-{checked}'
                subprocess.run([executable, 'init', '--quiet', f'--template={templates}', str(probe)],
                               check=True, env=env, umask=process_umask)
                for source_mode in source_modes:
                    name = f'{source_mode:04o}.sample'
                    observed = stat.S_IMODE((probe / '.git/hooks' / name).lstat().st_mode)
                    expected = git_template_copy_mode(source_mode, process_umask)
                    if observed != expected:
                        raise RuntimeError(f'{executable}: umask {process_umask:04o} copied {name} '
                                           f'as {observed:04o}, predicted {expected:04o}')
                checked += 1
    print(f'git template copy-mode self-test passed for {checked} init probes')

if sys.argv[1:2] == ['--self-test']:
    self_test(sys.argv[2:] or ['git'])
    sys.exit(0)

parser = argparse.ArgumentParser()
parser.add_argument('--root', required=True, type=Path)
parser.add_argument('--initialized-git', required=True, type=Path)
parser.add_argument('--initialized-umask', required=True, type=lambda value: int(value, 8))
parser.add_argument('--selected-git', required=True, type=Path)
parser.add_argument('--receipt', required=True, type=Path)
parser.add_argument('--expected-head', required=True)
args = parser.parse_args()
if not 0 <= args.initialized_umask <= 0o777:
    parser.error('initialized constructor umask is outside permission bits')

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
# setup-nix installs the job-scoped credential as exactly three owner-scoped
# numbered entries, in this order (metacraft-github-actions
# git-auth/scoped-git-auth.sh, _scoped_git_auth_helper_pairs, "THE JOB-SCOPED
# CREDENTIAL"): a preemptive-header reset, a helper reset, and the helper that
# reads the job's rotating token file. Admit nothing else: any other key,
# count, order, scope or helper command could select hooks, templates or a
# foreign credential. Keep the values only in task memory; path selectors and
# additional configuration stay refused. Governed by codetracer-pm
# spec/Testing/Trace-Nim-Checkout-Template-Constructor-Transition.md; contract
# suite ci/test-reconcile-checkout-templates.py.
auth_scope = 'https://github.com/metacraft-labs/'
job_token_helper = re.compile(r"!/bin/sh '/[^'\n]*/git-auth/git-credential-job-token\.sh' '/[^'\n]*'")
auth_keys = {name: value for name, value in os.environ.items()
            if name == 'GIT_CONFIG_COUNT' or name.startswith('GIT_CONFIG_KEY_')
            or name.startswith('GIT_CONFIG_VALUE_')}
if auth_keys:
    expected_names = {'GIT_CONFIG_COUNT'} | {f'GIT_CONFIG_{kind}_{index}'
                                              for kind in ['KEY', 'VALUE'] for index in range(3)}
    if (set(auth_keys) != expected_names
            or auth_keys['GIT_CONFIG_COUNT'] != '3'
            or [auth_keys[f'GIT_CONFIG_KEY_{index}'] for index in range(3)]
                != [f'http.{auth_scope}.extraHeader', f'credential.{auth_scope}.helper',
                    f'credential.{auth_scope}.helper']
            or auth_keys['GIT_CONFIG_VALUE_0'] != ''
            or auth_keys['GIT_CONFIG_VALUE_1'] != ''
            or not job_token_helper.fullmatch(auth_keys['GIT_CONFIG_VALUE_2'])):
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
    for executable, constructor_umask in [(old_git, args.initialized_umask), (git, 0o022)]:
        with tempfile.TemporaryDirectory(prefix='trace-git-template-probe-') as directory:
            probe = Path(directory) / 'probe'
            subprocess.run([str(executable), 'init', str(probe)], cwd=root, check=True,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, umask=constructor_umask)
            probe_payload = payload(samples(probe / '.git/hooks'))
            template_payload = payload(samples(Path(constructors[str(executable)]['templates']) / 'hooks'))
            # Keep exact source modes in constructors; predict the copy mode with
            # Git's own rule under the umask this probe ran with.
            template_payload = {name: [git_template_copy_mode(mode, constructor_umask), sha] for name, (mode, sha) in template_payload.items()}
            if probe_payload != template_payload:
                raise RuntimeError('Actual init differs from selected template supplier: ' + json.dumps({'git': str(executable), 'probeUmask': format(constructor_umask, '04o'), 'observed': probe_payload, 'expected': template_payload}, sort_keys=True))
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
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, umask=0o022)
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
