from pathlib import Path
import subprocess, os, json, time, hashlib, shutil, sys
base=Path(__file__).resolve().parent
source=Path(sys.argv[1]).resolve(); label=sys.argv[2]
package=sys.argv[3] if len(sys.argv)>3 else "stateknot-runtime"
target=base.parents[1]/"target"/(sys.argv[4] if len(sys.argv)>4 else "r1-oct15")
out=base/"evidence"/label; out.mkdir(parents=True,exist_ok=False)
env=os.environ.copy(); env.update(CARGO_TARGET_DIR=str(target),CARGO_INCREMENTAL="0",CARGO_PROFILE_DEV_DEBUG="0",CARGO_PROFILE_TEST_DEBUG="0",RUSTFLAGS="-D warnings",RUSTDOCFLAGS="-D warnings")
cmd=["cargo","test","--manifest-path",str(source/"Cargo.toml"),"-p",package,"--test","postgres","--all-features","--locked","--no-run","--message-format=json"]
start=time.time()
(out/"build-runner.py").write_bytes(Path(__file__).read_bytes())
cleaned=False
if len(sys.argv)>5 and sys.argv[5]=="--clean-workspace":
 assert target.resolve().parent==(base.parents[1]/"target").resolve() and target.name.startswith("r1-oct15")
 tag=target/"CACHEDIR.TAG"
 assert tag.is_file() and "Signature: 8a477f597d28d172789f06886806bc55" in tag.read_text()
 root=base.parents[1]
 tracked=subprocess.check_output(["git","ls-files","--",str(target.relative_to(root))],cwd=root)
 assert not tracked.strip(), "target unexpectedly contains tracked source"
 opened=subprocess.run(["lsof","+D",str(target)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 assert opened.returncode in [0,1] and not opened.stderr.strip(), "cannot establish cache inactivity"
 if opened.stdout.strip(): raise RuntimeError("task cache is active; skipped clean")
 (out/"cache-before.df").write_bytes(subprocess.check_output(["df","-k",str(target)]))
 packages=["stateknot","stateknot-artifact-store","stateknot-core","stateknot-integrations","stateknot-runtime","stateknot-store-postgres","stateknot-testkit"]
 clean=["cargo","clean","--manifest-path",str(source/"Cargo.toml"),"--target-dir",str(target)]
 for name in packages: clean += ["-p",name]
 with (out/"workspace-clean.log").open("w") as log: status=subprocess.run(clean,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
 if status.returncode!=0: raise RuntimeError("task workspace clean failed; see log")
 cleaned=True
 (out/"cache-after.df").write_bytes(subprocess.check_output(["df","-k",str(target)]))
with (out/"build.stdout.jsonl").open("w") as stdout, (out/"build.stderr.log").open("w") as stderr:
 result=subprocess.run(cmd,cwd=source,env=env,stdout=stdout,stderr=stderr)
artifacts=[]
if result.returncode==0:
 for line in (out/"build.stdout.jsonl").read_text().splitlines():
  try: event=json.loads(line)
  except json.JSONDecodeError: continue
  if event.get("reason")=="compiler-artifact" and event.get("executable") and event.get("target",{}).get("name")=="postgres":
   if cleaned and event.get("fresh"):
    raise RuntimeError("requested workspace artifact remained fresh after explicit clean")
   original=Path(event["executable"]); saved=out/(package+"-postgres")
   shutil.copy2(original,saved); artifacts.append({"path":str(saved),"sha256":hashlib.sha256(saved.read_bytes()).hexdigest()})
receipt={"source":str(source),"command":cmd,"exit_code":result.returncode,"seconds":time.time()-start,"target":str(target),"artifacts":artifacts,"workspace_artifacts_explicitly_cleaned":cleaned}
(out/"receipt.json").write_text(json.dumps(receipt,indent=2)+"\n")
print(json.dumps(receipt,indent=2)); sys.exit(result.returncode)
