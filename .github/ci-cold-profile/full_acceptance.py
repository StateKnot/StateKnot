"""Offline predicates only; no execution, artifact-cold or budget qualification by controls."""
from collections import Counter
import json,re
from mcp_text_marker import observe,validate,strict_step_lines

canonical=lambda v:json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False)
PREFIX=re.compile(r'^(STATEKNOT_[A-Z0-9_]+(?:EVIDENCE|REPORT))=(.*)$')
SUMMARY=re.compile(r'test result: ok[.] (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in [0-9.]+s')

def strict_json(text):
 def pairs(items):
  result={}
  for key,value in items:
   if key in result:raise ValueError('duplicate JSON key')
   result[key]=value
  return result
 def constant(_):raise ValueError('non-finite JSON')
 return json.loads(text,object_pairs_hook=pairs,parse_constant=constant)

def strip_timestamp(line):return re.sub(r'^\d{4}-\d\d-\d\dT[^ ]+Z ','',line)

def dynamic_ok(value,rule,major):
 if rule=='postgres_version':return isinstance(value,str) and re.match(str(major)+r'[.]\d+(?:\D|$)',value) is not None
 if rule=='sha256':return isinstance(value,str) and re.fullmatch('[0-9a-f]{64}',value) is not None
 if rule=='positive_integer':return type(value) is int and value>0
 if isinstance(rule,dict) and set(rule)=={'exact'}:return canonical(value)==canonical(rule['exact'])
 return False

def validate_values(values,definition,major,source=None):
 errors=[]
 if definition['kind']=='exact_text':
  if values!=[definition['value']]:errors.append('exact text marker changed/duplicated')
  return errors
 if definition['kind']=='integrity_envelope':
  if len(values)!=1:return ['integrity envelope count']
  try:envelope=strict_json(values[0])
  except (ValueError,TypeError):return ['integrity envelope JSON']
  if not isinstance(envelope,dict) or set(envelope)!={'schema_version','report','report_sha256'}:return ['integrity envelope closed fields']
  report=envelope['report']
  if type(envelope['schema_version']) is not int or envelope['schema_version']!=1 or not dynamic_ok(envelope['report_sha256'],'sha256',major):errors.append('integrity envelope version/hash shape')
  if not isinstance(report,dict):return errors+['report object']
  required={'schema_version':1,'profile':'ci_reduced','scenario':'agent_host_control_plane_v1','evidence_valid':True,'release_qualified':False}
  if any(k not in report or canonical(report[k])!=canonical(v) for k,v in required.items()):errors.append('report reduced/non-release contract')
  if source and report.get('environment',{}).get('source',{}).get('source_commit')!=source['commit']:errors.append('report source commit')
  if source and report.get('environment',{}).get('source',{}).get('source_tree')!=source['tree']:errors.append('report source tree')
  # Original exact Rust test must pass IntegrityEnvelope::from_json and all derived-field checks.
  # This Python predicate deliberately does not reimplement JCS/report evaluation.
  return errors
 if definition['kind']!='json_variants':return ['unknown marker predicate']
 actual=[];dynamic=definition['dynamic']
 for text in values:
  if not text.startswith('{') or not text.endswith('}'):errors.append('JSON line boundary/CR/suffix');continue
  try:value=strict_json(text)
  except (ValueError,TypeError):errors.append('strict JSON');continue
  if not isinstance(value,dict) or any(k not in value or not dynamic_ok(value[k],rule,major) for k,rule in dynamic.items()):errors.append('dynamic JSON fields');continue
  actual.append(canonical({k:v for k,v in value.items() if k not in dynamic}))
 if Counter(actual)!=Counter(canonical(v) for v in definition['variants']):errors.append('complete closed JSON variants/phase/cut matrix mismatch')
 return errors

def validate_block(lines,acceptance,definitions,major,source=None):
 errors=[];summaries=[];seen={};names=[]
 for line in lines:
  if line.startswith('test result:'):
   m=SUMMARY.fullmatch(line)
   if not m:errors.append('full LF libtest summary')
   else:summaries.append(dict(zip(['passed','failed','ignored','measured','filtered'],map(int,m.groups()))))
  m=re.match(r'^test (\S+) [.]{3}(?: |$)',line)
  if m:names.append(m[1])
  m=PREFIX.fullmatch(line)
  if m:seen.setdefault(m[1],[]).append(m[2])
 if summaries!=acceptance['summaries']:errors.append('exact summary inventory')
 wanted=[n for group in acceptance['selected_harness_names'] for n in group]
 if Counter(names)!=Counter(wanted):errors.append('exact selected harness name inventory')
 if set(seen)!=set(acceptance['markers']):errors.append('missing/unexpected old marker prefix')
 for prefix,count in acceptance['markers'].items():
  values=seen.get(prefix,[])
  if len(values)!=count:errors.append(prefix+': exact occurrence count')
  errors.extend(prefix+': '+e for e in validate_values(values,definitions[prefix],major,source))
 return errors

def validate_trace(raw,contract,definitions,major,source=None):
 errors=[];blocks={};active=None;terminals={}
 for rawline in raw.decode().split('\n'):
  line=strip_timestamp(rawline)
  if line.startswith('STATEKNOT_COLD_STEP_BEGIN='):
   try:label=strict_json(line.split('=',1)[1])['step']
   except (ValueError,KeyError,TypeError):errors.append('invalid step begin');continue
   if active is not None or label in blocks:errors.append('nested/replayed begin')
   active=label;blocks.setdefault(label,[])
  elif line.startswith('STATEKNOT_COLD_STEP_END='):
   try:end=strict_json(line.split('=',1)[1]);label=end['step']
   except (ValueError,KeyError,TypeError):errors.append('invalid step end');continue
   if label!=active or label in terminals:errors.append('unmatched/replayed terminal')
   terminals[label]=end;active=None
  elif active is not None:blocks[active].append(line)
 if active is not None:errors.append('unterminated last step')
 if set(blocks)!=set(contract['acceptance_by_run_label']):errors.append('full32 run body labels')
 for label,accept in contract['acceptance_by_run_label'].items():
  if label not in blocks:continue
  end=terminals.get(label,{})
  if end.get('complete') is not True or type(end.get('exit_code')) is not int or end.get('exit_code')!=0:errors.append(label+': terminal not complete/zero')
  errors.extend(label+': '+e for e in validate_block(blocks[label],accept,definitions,major,source))
  if label=='new-mcp':
   block={}
   for line in strict_step_lines(raw,'new-mcp'):observe(block,line)
   errors.extend('new-mcp: '+e for e in validate(block,accept,contract['mcp_expected_tests']))
 return errors
