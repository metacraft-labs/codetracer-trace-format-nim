"""Contract tests for the inherited Git authentication guard of
ci/reconcile-checkout-templates.py.

Each case runs the real reconciliation script, end to end, against a scratch
checkout that a real Nix-store Git initialized and committed, with the
`GIT_CONFIG_*` environment the case names. No mocks: the only stand-in is the
credential value itself. The admitted shape is the one
metacraft-github-actions `setup-nix` (and `refresh-build-auth`) export through
`$GITHUB_ENV` for the default `token-owner`, built by
`_scoped_git_auth_helper_pairs` in its `git-auth/scoped-git-auth.sh`:

    http.https://github.com/metacraft-labs/.extraHeader =         (reset)
    credential.https://github.com/metacraft-labs/.helper =        (reset)
    credential.https://github.com/metacraft-labs/.helper = !/bin/sh '<dir>/git-credential-job-token.sh' '<dir>'

with `$MCL_JOB_CREDENTIALS_DIR` naming the second `<dir>`. The guard also
requires the helper to be that action's own script, an existing regular file
at a normalized path under `_actions/metacraft-labs/metacraft-github-actions/`,
so the suite lays out a scratch runner work directory holding one, and
refuses each departure from it.

The governing contract is codetracer-pm
spec/Testing/Trace-Nim-Checkout-Template-Constructor-Transition.md.

Usage: python3 ci/test-reconcile-checkout-templates.py [--git <nix-store git>]
"""
from pathlib import Path
import argparse, json, os, shutil, subprocess, sys, tempfile

SCRIPT = Path(__file__).resolve().parent / 'reconcile-checkout-templates.py'
OWNER = 'https://github.com/metacraft-labs/'
HELPER_RELATIVE = '_actions/metacraft-labs/metacraft-github-actions/dev/git-auth/git-credential-job-token.sh'

def numbered(*pairs):
    env = {'GIT_CONFIG_COUNT': str(len(pairs))}
    for index, (key, value) in enumerate(pairs):
        env[f'GIT_CONFIG_KEY_{index}'] = key
        env[f'GIT_CONFIG_VALUE_{index}'] = value
    return env

def cases(work):
    """The accepted and refused environments for a scratch runner work
    directory ``work`` that holds the action's helper script and the job's
    credential directory, as a runner's `_work` does."""
    helper_path = work / HELPER_RELATIVE
    credentials = work / '_temp' / 'mcl-job-credentials'
    HELPER = f"!/bin/sh '{helper_path}' '{credentials}'"
    job = {'MCL_JOB_CREDENTIALS_DIR': str(credentials)}

    def with_job(*pairs):
        """``numbered`` beside the credential directory setup-nix exports."""
        return {**numbered(*pairs), **job}

    SETUP_NIX = [(f'http.{OWNER}.extraHeader', ''),
                 (f'credential.{OWNER}.helper', ''),
                 (f'credential.{OWNER}.helper', HELPER)]

    accept = {
        'no inherited numbered configuration': {},
        'setup-nix job-scoped credential helper': with_job(*SETUP_NIX),
    }
    refuse = {
        # The single preemptive header setup-nix used to emit. No setup emits it
        # any more; a header carries a token value that cannot rotate.
        'retired preemptive extraHeader': with_job(
            (f'http.{OWNER}.extraHeader', 'AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46Z2hz')),
        'credential header instead of the reset': with_job(
            (f'http.{OWNER}.extraHeader', 'AUTHORIZATION: basic eC1hY2Nlc3MtdG9rZW46Z2hz'),
            *SETUP_NIX[1:]),
        'additional hook selector': with_job(*SETUP_NIX, ('core.hooksPath', '/tmp/hooks')),
        'additional template selector': with_job(*SETUP_NIX, ('init.templateDir', '/tmp/templates')),
        'other owner scope': with_job(*[(key.replace('metacraft-labs', 'someone-else'), value)
                                        for key, value in SETUP_NIX]),
        'host-wide scope': with_job(*[(key.replace(OWNER, 'https://github.com/'), value)
                                      for key, value in SETUP_NIX]),
        'helper reset missing': with_job(SETUP_NIX[0], SETUP_NIX[2]),
        'nonempty helper reset': with_job(SETUP_NIX[0], (f'credential.{OWNER}.helper', 'store'),
                                          SETUP_NIX[2]),
        'host-wide helper key': with_job(*SETUP_NIX[:2], ('credential.helper', HELPER)),
        'reordered entries': with_job(SETUP_NIX[0], SETUP_NIX[2], SETUP_NIX[1]),
        'foreign helper command': with_job(*SETUP_NIX[:2], (f'credential.{OWNER}.helper', 'store')),
        'helper with trailing command': with_job(*SETUP_NIX[:2], (f'credential.{OWNER}.helper', HELPER + '; id')),
        'count larger than entries': {**with_job(*SETUP_NIX), 'GIT_CONFIG_COUNT': '4'},
        'stray numbered variable': {**with_job(*SETUP_NIX), 'GIT_CONFIG_KEY_7': 'core.hooksPath'},
    }
    # The guard's binding of the helper to this job and to the action's own
    # script.
    refuse.update({
        'credential directory of another job': {
            **with_job(*SETUP_NIX), 'MCL_JOB_CREDENTIALS_DIR': str(work / '_temp' / 'other')},
        'credential directory not exported': numbered(*SETUP_NIX),
        'helper outside the action checkout': with_job(*SETUP_NIX[:2], (
            f'credential.{OWNER}.helper',
            f"!/bin/sh '{work / 'elsewhere' / 'git-credential-job-token.sh'}' '{credentials}'")),
        'non-normalized helper path': with_job(*SETUP_NIX[:2], (
            f'credential.{OWNER}.helper',
            f"!/bin/sh '{helper_path.parent / '..' / 'git-auth' / helper_path.name}' '{credentials}'")),
        'helper that does not exist': with_job(*SETUP_NIX[:2], (
            f'credential.{OWNER}.helper',
            f"!/bin/sh '{helper_path.parent / 'missing.sh'}' '{credentials}'")),
    })
    # Selectors outside the numbered configuration are refused before the
    # shape is examined, even beside the admitted credential.
    refuse_authority = {
        'inherited GIT_DIR': {**with_job(*SETUP_NIX), 'GIT_DIR': '/tmp/foreign.git'},
        'inherited GIT_TEMPLATE_DIR': {**with_job(*SETUP_NIX), 'GIT_TEMPLATE_DIR': '/tmp/templates'},
        'inherited GIT_CONFIG_PARAMETERS': {**with_job(*SETUP_NIX),
                                            'GIT_CONFIG_PARAMETERS': "'core.hookspath'='/tmp/hooks'"},
    }
    return accept, refuse, refuse_authority

REFUSAL = 'Inherited Git authentication shape refused'
AUTHORITY_REFUSAL = 'Inherited Git authority refused'

def run_case(git, scratch, name, extra):
    root = scratch / 'checkout'
    if root.exists():
        shutil.rmtree(root)
    # Neither the caller's GIT_* nor its own job credential directory (CI runs
    # this suite inside a job that has one) reaches a case: each case names
    # what it inherits.
    base = {key: value for key, value in os.environ.items()
            if not key.startswith('GIT_') and key != 'MCL_JOB_CREDENTIALS_DIR'}
    base.update({'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1',
                 'HOME': str(scratch / 'home')})
    def plain(*argv):
        subprocess.run([str(git), *argv], cwd=root, env=base, check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, umask=0o022)
    root.mkdir()
    plain('init', '--quiet', str(root))
    (root / 'tracked.txt').write_text('tracked\n')
    plain('add', 'tracked.txt')
    plain('-c', 'user.name=test', '-c', 'user.email=test@invalid', 'commit', '--quiet', '-m', 'seed')
    head = subprocess.check_output([str(git), 'rev-parse', 'HEAD'], cwd=root, env=base).decode().strip()
    receipt = scratch / f'receipt-{len(list(scratch.glob("receipt-*")))}.json'
    result = subprocess.run(
        [sys.executable, str(SCRIPT), '--root', str(root), '--initialized-git', str(git),
         '--initialized-umask', '0022', '--selected-git', str(git), '--receipt', str(receipt),
         '--expected-head', head],
        cwd=root, env={**base, **extra}, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result, receipt

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--git', type=Path, default=shutil.which('git'))
    args = parser.parse_args()
    git = Path(args.git).resolve(strict=True)
    failures = []
    with tempfile.TemporaryDirectory(prefix='trace-reconcile-auth-test-') as directory:
        scratch = Path(directory)
        (scratch / 'home').mkdir()
        # A runner work directory: the action checkout's helper script (a
        # stand-in body; the guard runs no helper) and the job's credential
        # directory.
        work = scratch / '_work'
        helper = work / HELPER_RELATIVE
        helper.parent.mkdir(parents=True)
        helper.write_text('#!/bin/sh\nexit 0\n')
        helper.chmod(0o755)
        (work / '_temp' / 'mcl-job-credentials').mkdir(parents=True)
        ACCEPT, REFUSE, REFUSE_AUTHORITY = cases(work)
        for name, extra in ACCEPT.items():
            result, receipt = run_case(git, scratch, name, extra)
            success = receipt.exists() and json.loads(receipt.read_text()).get('success') is True
            if result.returncode != 0 or not success:
                failures.append(f'accept {name!r}: exit {result.returncode}: {result.stderr.strip()[-400:]}')
            elif any(value and value in receipt.read_text() for value in extra.values()
                     if value not in {'', '3'}):
                failures.append(f'accept {name!r}: the receipt carries an authentication value')
            else:
                print(f'ok accept {name}')
        refusals = [(name, extra, REFUSAL) for name, extra in REFUSE.items()]
        refusals += [(name, extra, AUTHORITY_REFUSAL) for name, extra in REFUSE_AUTHORITY.items()]
        for name, extra, refusal in refusals:
            result, receipt = run_case(git, scratch, name, extra)
            if result.returncode == 0 or refusal not in result.stderr:
                failures.append(f'refuse {name!r}: exit {result.returncode}: {result.stderr.strip()[-400:]}')
            elif receipt.exists():
                failures.append(f'refuse {name!r}: a receipt was written before the refusal')
            else:
                print(f'ok refuse {name}')
    if failures:
        print('\n'.join(['FAILED:', *failures]), file=sys.stderr)
        sys.exit(1)
    print(f'reconcile authentication contract: {len(ACCEPT)} accepted, {len(refusals)} refused')

if __name__ == '__main__':
    main()
