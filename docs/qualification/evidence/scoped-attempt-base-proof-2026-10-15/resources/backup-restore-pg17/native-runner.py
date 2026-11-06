from pathlib import Path
import os, subprocess, time, json, hashlib, sys, re
base=Path(__file__).resolve().parent
build=base/"evidence"/sys.argv[1]; label=sys.argv[2]; version=sys.argv[3]; name=sys.argv[4]; database=sys.argv[5]
assert re.fullmatch(r"stateknot_r1_[a-z0-9_]+",database)
receipt=json.loads((build/"receipt.json").read_text())
assert receipt["exit_code"]==0 and len(receipt["artifacts"])==1
artifact=receipt["artifacts"][0]; binary=Path(artifact["path"])
assert hashlib.sha256(binary.read_bytes()).hexdigest()==artifact["sha256"]
out=base/"evidence"/label; out.mkdir(parents=True,exist_ok=False)
env=os.environ.copy(); env.update(STATEKNOT_REQUIRE_POSTGRES_TESTS="1",STATEKNOT_TEST_DATABASE_URL="postgres://postgres:stateknot_test_password@127.0.0.1:554"+version+"/"+database)
assert "RUST_MIN_STACK" not in env
cmd=[str(binary),name,"--exact","--nocapture","--test-threads=1"]
start=time.time()
(out/"native-runner.py").write_bytes(Path(__file__).read_bytes())
with (out/"native.log").open("w") as log:
 process=subprocess.Popen(cmd,env=env,stdout=log,stderr=subprocess.STDOUT)
 (out/"running.json").write_text(json.dumps({"pid":process.pid,"started_unix":start,"binary_sha256":artifact["sha256"],"command":cmd},indent=2)+"\n")
 process.wait()
 result=process
log=(out/"native.log").read_text()
valid_summary=bool(re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;",log))
value={"expected_test_count":1,"valid_success_summary":valid_summary,"source":receipt["source"],"binary_sha256":artifact["sha256"],"command":cmd,"postgres_major":version,"database":database,"exit_code":result.returncode,"seconds":time.time()-start,"default_stack":True,"log_sha256":hashlib.sha256(log.encode()).hexdigest(),"result_lines":[x for x in log.splitlines() if "test result:" in x or "overflow" in x or "fatal runtime" in x]}
(out/"receipt.json").write_text(json.dumps(value,indent=2)+"\n")
print(json.dumps(value,indent=2)); sys.exit(0 if result.returncode==0 and valid_summary else 1)
