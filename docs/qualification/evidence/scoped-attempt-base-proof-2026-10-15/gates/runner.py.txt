from pathlib import Path
import os, subprocess, json, time, sys, re, hashlib
base=Path(__file__).resolve().parent; source=Path(sys.argv[1]).resolve(); label=sys.argv[2]; target=base.parents[1]/"target"/sys.argv[3]
out=base/"evidence"/label; out.mkdir(parents=True,exist_ok=False)
env=os.environ.copy(); env.update(CARGO_TARGET_DIR=str(target),CARGO_INCREMENTAL="0",CARGO_PROFILE_DEV_DEBUG="0",CARGO_PROFILE_TEST_DEBUG="0",RUSTFLAGS="-D warnings",RUSTDOCFLAGS="-D warnings")
for key in ["STATEKNOT_TEST_DATABASE_URL","STATEKNOT_REQUIRE_POSTGRES_TESTS"]: env.pop(key,None)
assert "RUST_MIN_STACK" not in env
(out/"gate-runner.py").write_bytes(Path(__file__).read_bytes())
def clean_workspace(cache,nightly=False):
 assert cache.resolve().parent==(base.parents[1]/"target").resolve() and cache.name.startswith("r1-oct15")
 if not cache.exists(): return
 assert "Signature: 8a477f597d28d172789f06886806bc55" in (cache/"CACHEDIR.TAG").read_text()
 assert not subprocess.check_output(["git","ls-files","--",str(cache.relative_to(base.parents[1]))],cwd=base.parents[1]).strip()
 opened=subprocess.run(["lsof","+D",str(cache)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
 assert opened.returncode in [0,1] and not opened.stderr.strip() and not opened.stdout.strip(), "cache active or inactivity could not be established"
 name="nightly" if nightly else "stable"
 (out/("cache-"+name+"-before.df")).write_bytes(subprocess.check_output(["df","-k",str(cache)]))
 cmd=["cargo"]+(["+nightly-2026-10-07"] if nightly else [])+["clean","--manifest-path",str(source/"Cargo.toml"),"--target-dir",str(cache)]
 for package in ["stateknot","stateknot-artifact-store","stateknot-core","stateknot-integrations","stateknot-runtime","stateknot-store-postgres","stateknot-testkit"]: cmd += ["-p",package]
 with (out/("cache-"+name+"-clean.log")).open("w") as log: result=subprocess.run(cmd,cwd=source,env=env,stdout=log,stderr=subprocess.STDOUT)
 assert result.returncode==0
 (out/("cache-"+name+"-after.df")).write_bytes(subprocess.check_output(["df","-k",str(cache)]))
clean_requested=len(sys.argv)>4 and sys.argv[4]=="--clean-workspace"
if clean_requested: clean_workspace(target)
commands=[("fmt",["cargo","fmt","--all","--check"]),("clippy",["cargo","clippy","--workspace","--all-targets","--all-features","--locked","--","-D","warnings"]),("workspace",["cargo","test","--workspace","--all-targets","--all-features","--locked"]),("doctests",["cargo","test","--workspace","--doc","--all-features","--locked"]),("rustdoc",["cargo","doc","--workspace","--all-features","--no-deps","--locked"]),("nightly",["cargo","+nightly-2026-10-07","check","--workspace","--all-targets","--all-features","--locked"])]
receipts=[]
for name,cmd in commands:
 start=time.time(); (out/"running.json").write_text(json.dumps({"gate":name,"started_unix":start,"command":cmd},indent=2)+"\n")
 gate_env=env.copy()
 if name=="nightly":
  gate_env["CARGO_TARGET_DIR"]=str(target)+"-nightly"
  if clean_requested: clean_workspace(Path(gate_env["CARGO_TARGET_DIR"]),True)
 with (out/(name+".log")).open("w") as log: result=subprocess.run(cmd,cwd=source,env=gate_env,stdout=log,stderr=subprocess.STDOUT)
 body=(out/(name+".log")).read_text()
 summaries=re.findall(r"test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;",body)
 row={"gate":name,"command":cmd,"exit_code":result.returncode,"seconds":time.time()-start,"log_sha256":hashlib.sha256(body.encode()).hexdigest(),"summaries":summaries,"target":gate_env["CARGO_TARGET_DIR"]}
 receipts.append(row); (out/"receipts.json").write_text(json.dumps({"source":str(source),"native_database_tests":False,"receipts":receipts},indent=2)+"\n")
 print(json.dumps({k:v for k,v in row.items() if k!="summaries"}),flush=True)
 if result.returncode!=0: sys.exit(1)
