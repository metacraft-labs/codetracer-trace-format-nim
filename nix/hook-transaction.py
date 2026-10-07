# Unix-only Nix-shell constructor. Process-group absence is bounded installer lifecycle
# evidence, not a claim about arbitrary children that deliberately create new sessions.
import base64, hashlib, json, os, pathlib, stat, subprocess, sys, tempfile, time
# The shell supplies exact immutable guard/factory/config/tool paths. No arbitrary
# hook text or Git configuration normalization is accepted by recovery.
root=pathlib.Path(sys.argv[1]); guard=sys.argv[2]; native=sys.argv[3]; templates=sys.argv[4]
legacy=sys.argv[5]; config=sys.argv[6]; legacy_config=sys.argv[7]; installer=sys.argv[8]
git_exe=sys.argv[9]; bash_exe=sys.argv[10]
prior_native=sys.argv[11]; prior_config=sys.argv[12]
args=[str(root),native,'snapshot','reserved',templates,legacy,config,legacy_config,prior_native,prior_config]
ctx=pathlib.Path(tempfile.mkdtemp(prefix='trace-nim-hook-transaction-'))
active=None; last_sid=None; stages=[]
def group_exists(pgid):
  # Signal zero only observes existence/permission; never signals a child.
  try: os.killpg(pgid,0)
  except ProcessLookupError: return False
  except PermissionError as exc: raise RuntimeError('Owned process-group census permission failure') from exc
  return True

def drain():
  global active,last_sid
  if active is not None:
    while True:
      try: active.wait(); break
      except KeyboardInterrupt: continue
  if last_sid is not None:
    while group_exists(last_sid):
      try: time.sleep(.1)
      except KeyboardInterrupt: continue
  active=None; last_sid=None

def run(label,argv,child_env=None):
  global active,last_sid
  item={'label':label,'argv':argv,'creation':'Popen start_new_session=True; direct waitpid-owned child','controller_uid':os.getuid()};stages.append(item)
  with (ctx/(label+'.stdout')).open('wb') as out,(ctx/(label+'.stderr')).open('wb') as err:
    active=subprocess.Popen(argv,cwd=root,stdout=out,stderr=err,start_new_session=True,env=child_env)
    last_sid=active.pid;item['pid']=active.pid;item['sid']=last_sid
    try:
      try:
        item['observed_pgid']=os.getpgid(active.pid)
        item['observed_sid']=os.getsid(active.pid)
        if item['observed_pgid']!=last_sid or item['observed_sid']!=last_sid: raise RuntimeError('Owned child group/session mismatch')
      except ProcessLookupError:
        observed=active.poll()
        if observed is None: raise RuntimeError('Missing live direct-child identity')
        item['already_terminal_observed_exit']=observed
      code=active.wait()
    finally: drain()
  item['exit']=code
  item['direct_child_reaped_and_process_group_absent']=True
  if code:
    if label in ('snapshot','prepare','tool','native-check','after','rollback-census'):
      diagnostic=(ctx/(label+'.stderr')).read_bytes()
      sys.stderr.buffer.write(diagnostic)
      sys.stderr.buffer.flush()
      requested=os.environ.get('TRACE_NIM_GUARD_DIAGNOSTIC_ROOT')
      if requested:
        expected=root/'.repro/constructor-binding/guard-stderr'
        if os.environ.get('GITHUB_WORKSPACE')!=str(root) or pathlib.Path(requested)!=expected:
          raise RuntimeError('Guard diagnostic output differs from the owning CI proof root; original guard failed')
        for ancestor in [expected,*expected.parents]:
          info=ancestor.lstat()
          if not stat.S_ISDIR(info.st_mode):
            raise RuntimeError('Guard diagnostic output has non-directory/symlink ancestry; original guard failed')
        observed=subprocess.check_output([git_exe,'-C',str(root),'rev-parse','--show-toplevel']).decode().strip()
        if observed!=str(root):
          raise RuntimeError('Guard diagnostic output is outside the owning Git root; original guard failed')
        with (expected/(label+'-'+str(os.getpid())+'.stderr')).open('xb') as failure_file:
          failure_file.write(diagnostic)
    raise RuntimeError(label+' failed; retained receipt '+str(ctx))
  return (ctx/(label+'.stdout')).read_bytes()

def plain_file(p):
  st=p.lstat()
  if not stat.S_ISREG(st.st_mode): raise RuntimeError('Unexpected file kind '+str(p))
  return {'mode':stat.S_IMODE(st.st_mode),'bytes':base64.b64encode(p.read_bytes()).decode()}

def git(*argv):
  return subprocess.check_output([git_exe,'-C',str(root),*argv])

initial=json.loads(run('snapshot',[sys.executable,guard,*args]))
common=pathlib.Path(git('rev-parse','--path-format=absolute','--git-common-dir').decode().strip())
hooks=common/'hooks';gitconfig=common/'config';index=git('ls-files','--stage','-z')
original={p.name:plain_file(p) for p in hooks.iterdir()}
original_config=plain_file(gitconfig)
config_link=root/'.pre-commit-config.yaml'
original_link=os.readlink(config_link) if config_link.is_symlink() else None
allowed_configs={base64.b64decode(original_config['bytes'])}
# Generate exact supported configuration transitions in private copies.
for unset in (False,True):
  for value in (None,'.git/hooks',str(hooks),os.path.relpath(hooks,root)):
    copy=ctx/('gitconfig-'+str(unset)+'-'+str(len(allowed_configs)))
    copy.write_bytes(base64.b64decode(original_config['bytes']))
    if unset:
      result=subprocess.run([git_exe,'config','--file',str(copy),'--unset-all','core.hooksPath'])
      if result.returncode not in (0,5): raise RuntimeError('Cannot derive exact Git config postimage')
    if value is not None: subprocess.run([git_exe,'config','--file',str(copy),'core.hooksPath',value],check=True)
    allowed_configs.add(copy.read_bytes())
snapshot={'initial':initial,'hooks':original,'gitconfig':original_config,'generated_config_link':original_link,'semantic_index':index.hex()}
(ctx/'snapshot.json').write_text(json.dumps(snapshot,indent=2)+'\n')
transaction=False; success=False; error=None
try:
  transaction=True
  receipt=run('prepare',[sys.executable,guard,*[str(root),native,'prepare','reserved',templates,legacy,config,legacy_config,prior_native,prior_config]])
  (ctx/'prepared.json').write_bytes(receipt)
  tool=run('tool',[sys.executable,guard,*[str(root),native,'tool','reserved',templates,legacy,config,legacy_config,prior_native,prior_config]]).decode().strip()
  needed=not(config_link.is_symlink() and os.readlink(config_link)==config)
  # Replace only exact prior standard native local slots under this snapshot.
  # Preflight the complete current state before the first retirement write.
  if original_link == prior_config:
    retire=[]
    for hook in ('pre-commit','pre-push'):
      local=hooks/(hook+'.repro-local')
      if local.exists():
        expected=plain_file(pathlib.Path(prior_native)/hook)
        expected['mode']=0o755  # measured installer mode; Nix freezes factory output to 0555
        if plain_file(local)!=expected: raise RuntimeError('Unknown prior native local slot')
        retire.append(local)
    if {p.name:plain_file(p) for p in hooks.iterdir()}!=original: raise RuntimeError('Hook inventory changed before prior retirement')
    if plain_file(gitconfig)!=original_config or git('ls-files','--stage','-z')!=index: raise RuntimeError('Git state changed before prior retirement')
    if not config_link.is_symlink() or os.readlink(config_link)!=original_link: raise RuntimeError('Generated config changed before prior retirement')
    for local in retire: local.unlink()
  native_global=ctx/'native-installer-empty-global'
  native_global.write_bytes(b'');native_global.chmod(0o600)
  native_env=dict(os.environ)
  # The Nix package supplies its own Python imports. Inherited shell imports
  # must not select an older pre_commit module through this exact wrapper.
  for authority in ('PYTHONPATH','PYTHONHOME','NIX_PYTHONPATH'):
    native_env.pop(authority,None)
  # Match the factory's declared Git/Bash principals when generating bodies.
  native_env['PATH']=str(pathlib.Path(git_exe).parent)+os.pathsep+str(pathlib.Path(bash_exe).parent)+os.pathsep+native_env.get('PATH','')
  native_env['GIT_CONFIG_GLOBAL']=str(native_global)
  native_env['GIT_CONFIG_NOSYSTEM']='1'
  run('native-install',[bash_exe,installer],native_env)
  if native_global.read_bytes()!=b'': raise RuntimeError('Native installer mutated private empty global')
  if needed: run('native-check',[sys.executable,guard,*[str(root),native,'native','reserved',templates,legacy,config,legacy_config,prior_native,prior_config]])
  run('managed-reconcile',[tool,'hooks','ensure','--vcs',str(root)])
  # Preserve original linked-worktree repair inside exact transaction authority.
  if git('config','--local','--get','core.hooksPath').decode().strip()=='.git/hooks':
    run('relative-to-common-hookspath',[git_exe,'-C',str(root),'config','--local','core.hooksPath',str(hooks)])
  run('after',[sys.executable,guard,*[str(root),native,'after',str(ctx/'prepared.json'),templates,legacy,config,legacy_config,prior_native,prior_config]])
  if git('ls-files','--stage','-z')!=index: raise RuntimeError('Semantic index changed')
  success=True
except BaseException as exc:
  error=repr(exc)
finally:
  rollback=None
  drain_error=None
  try: drain()
  except BaseException as exc:
    drain_error=repr(exc);success=False;error=error or drain_error
  if drain_error is not None:
    rollback='UNSAFE restoration refused: owned lifecycle unknown '+drain_error
  if transaction and not success and drain_error is None:
    try:
      # Guard admits only genuine bound bodies; it never restores unknown writes.
      run('rollback-census',[sys.executable,guard,*[str(root),native,'rollback','reserved',templates,legacy,config,legacy_config,prior_native,prior_config]])
      if git('ls-files','--stage','-z')!=index: raise RuntimeError('Unknown semantic index mutation; no restoration')
      current=plain_file(gitconfig)
      if current['mode']!=original_config['mode'] or base64.b64decode(current['bytes']) not in allowed_configs:
        raise RuntimeError('Unknown Git configuration mutation; no restoration')
      if config_link.exists() or config_link.is_symlink():
        if not config_link.is_symlink() or os.readlink(config_link) not in {config,legacy_config,original_link}:
          raise RuntimeError('Unknown generated configuration mutation; no restoration')
      # All checks complete before the first restoration write.
      for p in hooks.iterdir():
        if p.name not in original: p.unlink()
      for name,item in original.items():
        target=hooks/name; body=base64.b64decode(item['bytes'])
        if not target.exists() or target.read_bytes()!=body or stat.S_IMODE(target.stat().st_mode)!=item['mode']:
          target.write_bytes(body);target.chmod(item['mode'])
      if config_link.is_symlink(): config_link.unlink()
      if original_link is not None: config_link.symlink_to(original_link)
      if gitconfig.read_bytes()!=base64.b64decode(original_config['bytes']): gitconfig.write_bytes(base64.b64decode(original_config['bytes']))
      if {p.name:plain_file(p) for p in hooks.iterdir()}!=original: raise RuntimeError('Restored inventory mismatch')
      rollback='exact original inventory restored'
    except BaseException as exc: rollback='UNSAFE restoration refused: '+repr(exc)
  (ctx/'proof.json').write_text(json.dumps({'success':success,'error':error,'rollback':rollback,'stages':stages,'scope':'actual supported installer transaction; receipt retained'},indent=2)+'\n')
  print('TraceNim hook transaction receipt: '+str(ctx),file=sys.stderr)
if not success: sys.exit(1)
