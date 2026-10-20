-- Copyright 2026 StateKnot contributors
-- SPDX-License-Identifier: Apache-2.0

-- Retain the exact original publication of one unconsumed scoped Join owner.
-- V1 and registered-unpublished V2 dispositions retain their original guards.
-- V3 additionally owns the full binding and publication; it never creates a
-- result, consumption, ordinary completion or execution authority. The complete
-- caller/account/deferred guard and 54-table ACL inventory remain unchanged.
-- Append-only upgrade, no backfill.
LOCK TABLE stateknot.runs IN SHARE ROW EXCLUSIVE MODE;

CREATE OR REPLACE FUNCTION stateknot.guard_graph_frame_closure_complete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE whole stateknot.graph_frame_closures%ROWTYPE; wire jsonb; claim jsonb; completion jsonb;
 previous text:=''; actual_count bigint; position integer;
 stop jsonb; owner jsonb; registration jsonb; joined stateknot.child_run_joins%ROWTYPE;
 request jsonb; account jsonb; child_entry jsonb; member jsonb; evidence jsonb; terminal jsonb;
 owned stateknot.child_run_ownership%ROWTYPE; settled stateknot.child_run_settlements%ROWTYPE;
 child_terminal stateknot.child_run_terminals%ROWTYPE; child_admission stateknot.agent_admissions%ROWTYPE;
 stopped_attempt uuid; member_count bigint; previous_slot text;
 published stateknot.child_run_join_bindings%ROWTYPE; binding jsonb; publication jsonb;
 terminal_bytes text; binding_digest bytea;
BEGIN
 SELECT * INTO whole FROM stateknot.graph_frame_closures WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id;
 IF NOT FOUND THEN RAISE EXCEPTION 'closed caller requires its whole closure' USING ERRCODE='SKG02'; END IF;
 wire:=convert_from(whole.closure_bytes,'UTF8')::jsonb;
 IF ((wire->>'version'='1' AND wire->'intent'->>'version'='1' AND NOT (wire->'intent' ? 'stopped_join'))
   OR (wire->>'version'='2' AND wire->'intent'->>'version'='2'
    AND jsonb_typeof(wire->'intent'->'stopped_join')='object')
   OR (wire->>'version'='3' AND wire->'intent'->>'version'='3'
    AND jsonb_typeof(wire->'intent'->'stopped_join')='object')) IS NOT TRUE
  OR jsonb_typeof(wire->'intent'->'frames') IS DISTINCT FROM 'array' OR jsonb_typeof(wire->'completions') IS DISTINCT FROM 'array'
 THEN RAISE EXCEPTION 'invalid whole closure wire' USING ERRCODE='SKG02'; END IF;
 IF jsonb_array_length(wire->'intent'->'frames')<>whole.frame_count OR jsonb_array_length(wire->'completions')<>whole.frame_count
  OR (wire->'intent'->'direct_usage'->>'unpriced_cost_events')::bigint IS DISTINCT FROM 0
  OR NOT EXISTS (
   SELECT 1 FROM stateknot.runs r JOIN stateknot.graph_frame_stacks s USING(tenant_id,run_id)
   JOIN stateknot.run_events v ON v.tenant_id=r.tenant_id AND v.run_id=r.run_id AND v.sequence=whole.journal_sequence
   JOIN stateknot.run_checkpoints p ON p.tenant_id=r.tenant_id AND p.run_id=r.run_id AND p.checkpoint_id=whole.root_checkpoint_id
   WHERE r.tenant_id=whole.tenant_id AND r.run_id=whole.run_id AND r.quarantined_at IS NULL
    AND r.lease_attempt_id IS NULL AND r.checkpoint_id=whole.root_checkpoint_id AND r.checkpoint_superstep=whole.root_superstep AND r.checkpoint_digest=whole.root_checkpoint_digest
    AND p.graph_namespace='' AND p.frame_identity_digest IS NULL AND p.superstep=whole.root_superstep AND p.checkpoint_digest=whole.root_checkpoint_digest
    AND r.journal_sequence=whole.journal_sequence AND r.journal_digest=whole.journal_digest
    AND convert_from(r.lifecycle_bytes,'UTF8')::jsonb=wire->'intent'->'lifecycle'
    AND ((r.lifecycle_status='cancellation_requested' AND wire->'intent'->'failure_close'='null'::jsonb
      AND wire->'intent'->'failure'=convert_from(r.lifecycle_bytes,'UTF8')::jsonb#>'{state,request,failure}')
     OR (r.lifecycle_status='active' AND EXISTS (SELECT 1 FROM stateknot.run_failure_closes f WHERE f.tenant_id=r.tenant_id AND f.run_id=r.run_id AND f.completed_at IS NULL
      AND convert_from(f.intent_bytes,'UTF8')::jsonb->'failure'=wire->'intent'->'failure'
      AND convert_from(f.intent_bytes,'UTF8')::jsonb->'direct_usage'=wire->'intent'->'direct_usage'
      AND f.journal_digest=decode(substr(wire->'intent'->'failure_close'->>'digest',8),'hex'))))
    AND s.active_namespace='' AND s.active_frame_identity_digest IS NULL AND s.lifetime_starts=whole.lifetime_starts AND s.admission_digest=whole.admission_digest
    AND v.source_kind='control_plane' AND v.event_kind='graph-frames-closed' AND v.projection_digest=whole.compound_digest
    AND v.event_id=whole.journal_event_id AND v.recorded_at=whole.journal_recorded_at AND v.event_digest=whole.journal_digest
  ) THEN RAISE EXCEPTION 'incomplete whole frame closure decision or projection' USING ERRCODE='SKG02'; END IF;

 -- V2 retains its unpublished registration; V3 retains the original complete
 -- publication. Neither makes another unfinished ordinary node safe.
 IF wire->>'version' IN ('2','3') THEN
  stop:=wire->'intent'->'stopped_join'; owner:=stop->'owner'; registration:=stop->'registration';
  IF (jsonb_typeof(owner)='object' AND jsonb_typeof(registration)='object'
   AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(wire) key)=ARRAY['completions','event','intent','version']::text[]
   AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(wire->'intent') key)
       =ARRAY['admission_digest','caller_failure','child_account','delegated_usage','direct_usage','failure','failure_close','frames','lifecycle','lifetime_starts','observed_head','root','stopped_join','version']::text[]
   AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(owner) key)=ARRAY['activation','attempt_id','digest','fence','journal_head']::text[]
   AND ((wire->>'version'='2'
     AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(stop) key)
         =ARRAY['owner','registration','request_digest']::text[])
    OR (wire->>'version'='3'
     AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(stop) key)
         =ARRAY['binding','owner','publication','registration','request_digest']::text[]))
   AND wire->'intent'->'failure_close'='null'::jsonb
   AND wire->'intent'->'lifecycle'->'state'->>'kind'='cancellation_requested'
   AND owner->'activation'->>'graph_namespace'=whole.active_namespace
   AND whole.active_namespace<>''
   AND registration->>'tenant_id'=whole.tenant_id AND registration->>'run_id'=whole.run_id::text
   AND owner->'fence'->>'tenant_id'=whole.tenant_id AND owner->'fence'->>'run_id'=whole.run_id::text
   AND owner->'journal_head'->>'tenant_id'=whole.tenant_id AND owner->'journal_head'->>'run_id'=whole.run_id::text
   AND owner->'activation'->'base_checkpoint'->>'tenant_id'=whole.tenant_id
   AND owner->'activation'->'base_checkpoint'->>'run_id'=whole.run_id::text
   AND wire->'intent'->'observed_head'->>'tenant_id'=whole.tenant_id
   AND wire->'intent'->'observed_head'->>'run_id'=whole.run_id::text
   AND (wire->'intent'->'observed_head'->>'sequence')::bigint=whole.journal_sequence-1
   AND EXISTS (SELECT 1 FROM stateknot.run_events observed
    WHERE observed.tenant_id=whole.tenant_id AND observed.run_id=whole.run_id
     AND observed.sequence=(wire->'intent'->'observed_head'->>'sequence')::bigint
     AND observed.event_id::text=wire->'intent'->'observed_head'->>'event_id'
     AND observed.recorded_at=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz
     AND observed.event_digest=decode(substr(wire->'intent'->'observed_head'->>'digest',8),'hex'))
   AND EXISTS (SELECT 1 FROM stateknot.run_events closure_event
    WHERE closure_event.tenant_id=whole.tenant_id AND closure_event.run_id=whole.run_id
     AND closure_event.sequence=whole.journal_sequence AND closure_event.source_kind='control_plane'
     AND ((wire->>'version'='2'
       AND closure_event.schema_id='https://stknot.com/schemas/store/graph-frame-closure-event/2.0.0'
       AND closure_event.schema_version='2.0.0'
       AND convert_from(closure_event.payload_bytes,'UTF8')::jsonb->'data'=
        jsonb_build_object('version',2,'intent_digest','sha256:'||encode(whole.intent_digest,'hex'),'frame_count',whole.frame_count))
      OR (wire->>'version'='3'
       AND closure_event.schema_id='https://stknot.com/schemas/store/graph-frame-closure-event/3.0.0'
       AND closure_event.schema_version='3.0.0'
       AND convert_from(closure_event.payload_bytes,'UTF8')::jsonb->'data'=
        jsonb_build_object('version',3,'intent_digest','sha256:'||encode(whole.intent_digest,'hex'),'frame_count',whole.frame_count))))
  ) IS NOT TRUE THEN RAISE EXCEPTION 'invalid registered Join stop disposition' USING ERRCODE='SKG02'; END IF;

  SELECT * INTO joined FROM stateknot.child_run_joins j
   WHERE j.tenant_id=whole.tenant_id AND j.parent_run_id=whole.run_id
    AND j.request_digest=decode(substr(stop->>'request_digest',8),'hex');
  IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop has no exact registration' USING ERRCODE='SKG02'; END IF;
  request:=convert_from(joined.request_bytes,'UTF8')::jsonb;
  stopped_attempt:=joined.node_attempt_id;
  IF (joined.graph_namespace=whole.active_namespace
   AND ((wire->>'version'='2' AND joined.ready_at IS NULL)
    OR (wire->>'version'='3' AND joined.ready_at IS NOT NULL)) AND joined.consumed_at IS NULL
   AND joined.node_attempt_id::text=owner->>'attempt_id'
   AND joined.base_checkpoint_id::text=owner#>>'{activation,base_checkpoint,checkpoint_id}'
   AND joined.node_id=owner#>>'{activation,node_id}'
   AND joined.graph_namespace=owner#>>'{activation,graph_namespace}'
   AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(request) key)=ARRAY['activation_digest','digest','keys','version']::text[]
   AND request->>'version'='1' AND request->>'digest'=stop->>'request_digest'
   AND request->>'activation_digest'='sha256:'||encode(joined.activation_digest,'hex')
   AND joined.request_checksum=sha256(joined.request_bytes)
   AND jsonb_typeof(request->'keys')='array' AND jsonb_array_length(request->'keys') BETWEEN 1 AND 64
   AND registration=jsonb_build_object('tenant_id',joined.tenant_id,'run_id',joined.parent_run_id::text,
    'sequence',joined.journal_sequence::text,'event_id',joined.journal_event_id::text,
    'recorded_at',to_char(joined.journal_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
    'digest','sha256:'||encode(joined.journal_digest,'hex'))
   AND joined.journal_sequence<whole.journal_sequence
   AND joined.journal_sequence<=(wire->'intent'->'observed_head'->>'sequence')::bigint
   AND joined.journal_recorded_at<=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz
   AND (wire->>'version'='3' OR NOT EXISTS (SELECT 1 FROM stateknot.child_run_join_bindings b WHERE b.tenant_id=joined.tenant_id AND b.parent_run_id=joined.parent_run_id AND b.activation_digest=joined.activation_digest))
   AND NOT EXISTS (SELECT 1 FROM stateknot.child_run_join_consumptions c WHERE c.tenant_id=joined.tenant_id AND c.parent_run_id=joined.parent_run_id AND c.activation_digest=joined.activation_digest)
   AND EXISTS (
    SELECT 1 FROM stateknot.node_attempts n
    JOIN stateknot.graph_frame_heads h ON h.tenant_id=n.tenant_id AND h.run_id=n.run_id AND h.graph_namespace=n.graph_namespace
    JOIN stateknot.run_checkpoints p ON p.tenant_id=n.tenant_id AND p.run_id=n.run_id AND p.graph_namespace=n.graph_namespace AND p.checkpoint_id=n.base_checkpoint_id
    JOIN stateknot.run_events started ON started.tenant_id=n.tenant_id AND started.run_id=n.run_id AND started.sequence=n.journal_sequence
    JOIN stateknot.run_events registered ON registered.tenant_id=joined.tenant_id AND registered.run_id=joined.parent_run_id AND registered.sequence=joined.journal_sequence
    JOIN stateknot.graph_definitions g ON g.tenant_id=n.tenant_id
     AND g.owner_issuer=owner#>>'{activation,base_checkpoint,graph,identity,owner,issuer}'
     AND g.owner_subject=owner#>>'{activation,base_checkpoint,graph,identity,owner,subject}'
     AND g.graph_name=owner#>>'{activation,base_checkpoint,graph,identity,capability,name}'
     AND g.graph_version=owner#>>'{activation,base_checkpoint,graph,identity,capability,version}'
     AND g.definition_digest=decode(substr(owner#>>'{activation,base_checkpoint,graph,definition_digest}',8),'hex')
    WHERE n.tenant_id=whole.tenant_id AND n.run_id=whole.run_id AND n.attempt_id=joined.node_attempt_id
     AND n.activation_digest=joined.activation_digest AND n.node_id=joined.node_id
     AND n.graph_namespace=joined.graph_namespace AND n.base_checkpoint_id=joined.base_checkpoint_id
     AND n.base_superstep=p.superstep AND n.base_checkpoint_digest=p.checkpoint_digest
     AND n.base_journal_sequence=p.journal_sequence AND n.base_journal_event_id=p.journal_event_id
     AND n.base_journal_recorded_at=p.journal_recorded_at AND n.base_journal_digest=p.journal_digest
     AND owner=(convert_from(n.start_bytes,'UTF8')::jsonb-'activation_digest')
     AND convert_from(n.start_bytes,'UTF8')::jsonb->>'activation_digest'='sha256:'||encode(n.activation_digest,'hex')
     AND owner->>'digest'='sha256:'||encode(n.start_digest,'hex')
     AND owner#>>'{activation,input_digest}'='sha256:'||encode(n.activation_input_digest,'hex')
     AND owner#>'{activation,base_checkpoint}'=(convert_from(p.checkpoint_bytes,'UTF8')::jsonb-ARRAY['state','ready_nodes','parent','intent_digest'])
     AND owner->'journal_head'=jsonb_build_object('tenant_id',n.tenant_id,'run_id',n.run_id::text,'sequence',n.journal_sequence::text,
      'event_id',n.journal_event_id::text,'recorded_at',to_char(n.journal_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(n.journal_digest,'hex'))
     AND owner->'fence'=jsonb_build_object('tenant_id',n.tenant_id,'run_id',n.run_id::text,'attempt_id',n.fence_attempt_id::text,'epoch',n.fence_epoch::text)
     AND h.checkpoint_id=p.checkpoint_id AND h.superstep=p.superstep AND h.checkpoint_digest=p.checkpoint_digest
     AND h.frame_identity_digest=whole.active_frame_identity_digest
     AND wire->'intent'->'frames'->(whole.frame_count-1)->'checkpoint'=convert_from(p.frame_checkpoint_head_bytes,'UTF8')::jsonb
     AND convert_from(p.checkpoint_bytes,'UTF8')::jsonb->'ready_nodes' ? n.node_id
     AND EXISTS (SELECT 1 FROM jsonb_array_elements(convert_from(g.definition_bytes,'UTF8')::jsonb->'nodes') node WHERE node->>'node_id'=n.node_id)
     AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(COALESCE(convert_from(g.definition_bytes,'UTF8')::jsonb#>'{frame_calls,calls}','[]'::jsonb)) call WHERE call->>'node_id'=n.node_id)
     AND started.event_id=n.journal_event_id AND started.recorded_at=n.journal_recorded_at AND started.event_digest=n.journal_digest
     AND started.source_kind='worker' AND started.worker_attempt_id=n.fence_attempt_id AND started.worker_epoch=n.fence_epoch
     AND started.projection_digest=n.start_digest
     AND n.journal_sequence<joined.journal_sequence AND n.journal_recorded_at<=joined.journal_recorded_at
     AND registered.event_id=joined.journal_event_id AND registered.recorded_at=joined.journal_recorded_at AND registered.event_digest=joined.journal_digest
     AND registered.event_kind='child-join-registered' AND registered.source_kind='worker'
     AND registered.worker_attempt_id=n.fence_attempt_id AND registered.worker_epoch=n.fence_epoch
     AND registered.projection_digest=sha256(convert_to('["stateknot.child-join.v1","registration","sha256:'||encode(joined.request_digest,'hex')||'","sha256:'||encode(n.start_digest,'hex')||'","sha256:'||encode(registered.intent_digest,'hex')||'"]','UTF8'))
     -- A retry in the same worker epoch is still a later physical Start.
     AND NOT EXISTS (SELECT 1 FROM stateknot.node_attempts later WHERE later.tenant_id=n.tenant_id AND later.run_id=n.run_id
      AND later.journal_sequence>n.journal_sequence AND (later.activation_digest=n.activation_digest
       OR (later.base_checkpoint_id=n.base_checkpoint_id AND later.base_superstep=n.base_superstep AND later.base_checkpoint_digest=n.base_checkpoint_digest
        AND later.graph_namespace=n.graph_namespace AND later.node_id=n.node_id AND later.activation_input_digest=n.activation_input_digest)))
     AND NOT EXISTS (SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id)
     AND NOT EXISTS (SELECT 1 FROM stateknot.pending_node_results result WHERE result.tenant_id=n.tenant_id AND result.run_id=n.run_id
      AND result.base_checkpoint_id=n.base_checkpoint_id AND result.graph_namespace=n.graph_namespace AND result.node_id=n.node_id)
     AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=n.tenant_id AND e.run_id=n.run_id AND e.caller_attempt_id=n.attempt_id)
     AND NOT EXISTS (SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=n.tenant_id AND b.run_id=n.run_id AND b.caller_attempt_id=n.attempt_id)
   )
  ) IS NOT TRUE THEN RAISE EXCEPTION 'registered Join stop owner or registration mismatch' USING ERRCODE='SKG02'; END IF;

  -- Prove both directions over every declared key. A nonempty prefix or a
  -- digest-only match cannot stand in for the registered 1..64-member set.
  SELECT count(*) INTO member_count FROM stateknot.child_run_ownership o
   WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id AND o.activation_digest=joined.activation_digest;
  IF member_count<>jsonb_array_length(request->'keys')
   OR (SELECT count(DISTINCT key->>'digest') FROM jsonb_array_elements(request->'keys') key)<>member_count
   OR (SELECT count(DISTINCT key->>'slot') FROM jsonb_array_elements(request->'keys') key)<>member_count
  THEN RAISE EXCEPTION 'registered Join stop membership is incomplete' USING ERRCODE='SKG02'; END IF;
  previous_slot:=NULL;
  FOR member IN SELECT value FROM jsonb_array_elements(request->'keys') LOOP
   IF ((SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(member) key)=ARRAY['digest','parent','slot']::text[]
    AND member->'parent'=owner->'activation' AND jsonb_typeof(member->'slot')='string'
    AND (previous_slot IS NULL OR (previous_slot COLLATE "C")<((member->>'slot') COLLATE "C"))) IS NOT TRUE
   THEN RAISE EXCEPTION 'registered Join stop key scope or order mismatch' USING ERRCODE='SKG02'; END IF;
   SELECT * INTO owned FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id
    AND o.key_digest=decode(substr(member->>'digest',8),'hex') AND o.activation_digest=joined.activation_digest;
   IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop key has no ownership' USING ERRCODE='SKG02'; END IF;
   IF (convert_from(owned.key_bytes,'UTF8')::jsonb=member AND owned.child_slot=member->>'slot'
    AND owned.parent_graph_namespace=joined.graph_namespace AND owned.parent_checkpoint_id=joined.base_checkpoint_id
    AND owned.parent_checkpoint_superstep=(owner#>>'{activation,base_checkpoint,superstep}')::bigint
    AND owned.parent_checkpoint_digest=decode(substr(owner#>>'{activation,base_checkpoint,digest}',8),'hex')
    AND owned.journal_sequence<joined.journal_sequence AND owned.settled AND owned.terminal_pending_at IS NULL) IS NOT TRUE
   THEN RAISE EXCEPTION 'registered Join stop ownership mismatch' USING ERRCODE='SKG02'; END IF;
   previous_slot:=member->>'slot';
  END LOOP;
  IF EXISTS (SELECT 1 FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id
   AND o.activation_digest=joined.activation_digest AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(request->'keys') key
    WHERE key=convert_from(o.key_bytes,'UTF8')::jsonb AND key->'parent'=owner->'activation'))
  THEN RAISE EXCEPTION 'registered Join stop has undeclared ownership' USING ERRCODE='SKG02'; END IF;

  IF wire->>'version'='3' THEN
   binding:=stop->'binding'; publication:=stop->'publication';
   SELECT * INTO published FROM stateknot.child_run_join_bindings b WHERE b.tenant_id=joined.tenant_id
    AND b.parent_run_id=joined.parent_run_id AND b.activation_digest=joined.activation_digest;
   IF NOT FOUND THEN RAISE EXCEPTION 'published Join stop has no exact publication' USING ERRCODE='SKG02'; END IF;
   -- The binding digest uses the Core NUL-separated domain and the canonical
   -- [request digest, COMPLETE ordered settlements] tuple. Reconstruct bytes
   -- from immutable settlement rows, never from jsonb::text or a proof prefix.
   SELECT '['||COALESCE(string_agg(convert_from(s.settlement_bytes,'UTF8'),',' ORDER BY o.child_slot COLLATE "C"),'')||']'
    INTO terminal_bytes FROM stateknot.child_run_ownership o
    JOIN stateknot.child_run_settlements s USING(tenant_id,parent_run_id,key_digest)
    WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id AND o.activation_digest=joined.activation_digest;
   binding_digest:=sha256(convert_to('stateknot.child-join-binding.v1','UTF8')||decode('00','hex')
    ||convert_to('["sha256:'||encode(joined.request_digest,'hex')||'",'||terminal_bytes||']','UTF8'));
   IF (jsonb_typeof(binding)='object' AND jsonb_typeof(publication)='object'
    AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(binding) key)=ARRAY['digest','request','terminals']::text[]
    AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(publication) key)=ARRAY['activation','binding_digest','journal_head','request_digest']::text[]
    AND jsonb_typeof(binding->'terminals')='array' AND jsonb_array_length(binding->'terminals')=member_count
    AND binding->'request'=request AND binding->'terminals'=terminal_bytes::jsonb
    AND binding->>'digest'='sha256:'||encode(binding_digest,'hex')
    AND published.binding_checksum=sha256(published.binding_bytes)
    AND published.binding_bytes=convert_to('{"digest":"sha256:'||encode(binding_digest,'hex')
     ||'","request":'||convert_from(joined.request_bytes,'UTF8')||',"terminals":'||terminal_bytes||'}','UTF8')
    AND binding=convert_from(published.binding_bytes,'UTF8')::jsonb
    AND publication=convert_from(published.head_bytes,'UTF8')::jsonb
    AND publication->'activation'=owner->'activation'
    AND publication->>'request_digest'=stop->>'request_digest'
    AND publication->>'binding_digest'='sha256:'||encode(binding_digest,'hex')
    AND publication->'journal_head'=jsonb_build_object('tenant_id',published.tenant_id,'run_id',published.parent_run_id::text,
     'sequence',published.journal_sequence::text,'event_id',published.journal_event_id::text,
     'recorded_at',to_char(published.journal_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
     'digest','sha256:'||encode(published.journal_digest,'hex'))
    AND joined.ready_at=published.ready_at AND published.ready_at>=joined.queued_at
    AND published.ready_at<=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz
    AND published.journal_sequence>joined.journal_sequence AND published.journal_recorded_at>=joined.journal_recorded_at
    AND published.journal_sequence<=(wire->'intent'->'observed_head'->>'sequence')::bigint
    AND published.journal_recorded_at<=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz
    AND NOT EXISTS (SELECT 1 FROM stateknot.child_run_ownership o
     JOIN stateknot.child_run_settlements s USING(tenant_id,parent_run_id,key_digest)
     WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id AND o.activation_digest=joined.activation_digest
      AND (s.journal_sequence>=published.journal_sequence OR s.journal_recorded_at>published.journal_recorded_at
       OR (convert_from(s.settlement_bytes,'UTF8')::jsonb#>>'{terminal,recorded_at}')::timestamptz>published.journal_recorded_at))
    AND EXISTS (SELECT 1 FROM stateknot.run_events event WHERE event.tenant_id=published.tenant_id
     AND event.run_id=published.parent_run_id AND event.sequence=published.journal_sequence
     AND event.event_id=published.journal_event_id AND event.recorded_at=published.journal_recorded_at AND event.event_digest=published.journal_digest
     AND event.source_kind='control_plane' AND event.event_kind='child-join-published'
     AND event.projection_digest=sha256(convert_to('["stateknot.child-join.v1","publication","sha256:'
      ||encode(joined.request_digest,'hex')||'","sha256:'||encode(binding_digest,'hex')
      ||'","sha256:'||encode(event.intent_digest,'hex')||'"]','UTF8')))
   ) IS NOT TRUE THEN RAISE EXCEPTION 'published Join stop publication or binding mismatch' USING ERRCODE='SKG02'; END IF;
  END IF;

  -- The complete account is settled and priced, including children belonging
  -- to other activations. Compare each complete settlement payload with the
  -- actual admitted child, immutable terminal and account entry.
  SELECT convert_from(a.account_bytes,'UTF8')::jsonb INTO account FROM stateknot.child_run_budget_accounts a
   JOIN stateknot.run_events event ON event.tenant_id=a.tenant_id AND event.run_id=a.parent_run_id AND event.sequence=a.journal_sequence
    AND event.event_id=a.journal_event_id AND event.recorded_at=a.journal_recorded_at AND event.event_digest=a.journal_digest
   WHERE a.tenant_id=whole.tenant_id AND a.parent_run_id=whole.run_id
    AND a.account_digest=decode(substr(wire->'intent'->'child_account'->>'digest',8),'hex')
    AND a.journal_sequence<=(wire->'intent'->'observed_head'->>'sequence')::bigint
    AND a.journal_recorded_at<=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz;
  IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop requires its complete account' USING ERRCODE='SKG02'; END IF;
  IF (account->>'version'='1' AND account->>'digest'=wire->'intent'->'child_account'->>'digest'
   AND account->'direct_head'=wire->'intent'->'child_account'->'direct_head'
   AND account->'direct_usage'=wire->'intent'->'child_account'->'direct_usage'
   AND account->>'parent_admission_digest'='sha256:'||encode(whole.admission_digest,'hex')
   AND account->'parent'->>'tenant_id'=whole.tenant_id AND account->'parent'->>'run_id'=whole.run_id::text
   AND EXISTS (SELECT 1 FROM stateknot.agent_admissions parent WHERE parent.tenant_id=whole.tenant_id AND parent.run_id=whole.run_id
    AND parent.admission_digest=whole.admission_digest
    AND account->'parent'=convert_from(parent.admission_bytes,'UTF8')::jsonb#>'{intent,provenance}'
    AND account->>'parent_admitted_at'=to_char(parent.admitted_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND account->'graph'=convert_from(parent.admission_bytes,'UTF8')::jsonb#>'{intent,graph}'
    AND account->'budget'=convert_from(parent.admission_bytes,'UTF8')::jsonb#>'{intent,budget}')
   AND jsonb_typeof(account->'children')='array' AND jsonb_array_length(account->'children') BETWEEN 1 AND 256
   AND account->'direct_usage'->>'unpriced_cost_events'='0'
   AND wire->'intent'->'delegated_usage'->>'unpriced_cost_events'='0'
   AND (account->'direct_head'->>'sequence')::bigint<=(wire->'intent'->'observed_head'->>'sequence')::bigint
   AND EXISTS (SELECT 1 FROM stateknot.run_events direct_event WHERE direct_event.tenant_id=whole.tenant_id AND direct_event.run_id=whole.run_id
    AND account->'direct_head'=jsonb_build_object('tenant_id',direct_event.tenant_id,'run_id',direct_event.run_id::text,'sequence',direct_event.sequence::text,
     'event_id',direct_event.event_id::text,'recorded_at',to_char(direct_event.recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(direct_event.event_digest,'hex')))
  ) IS NOT TRUE THEN RAISE EXCEPTION 'registered Join stop account observation mismatch' USING ERRCODE='SKG02'; END IF;
  SELECT count(*) INTO member_count FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id;
  IF member_count<>jsonb_array_length(account->'children')
   OR (SELECT count(DISTINCT entry->'key'->>'digest') FROM jsonb_array_elements(account->'children') entry)<>member_count
  THEN RAISE EXCEPTION 'registered Join stop account ownership is incomplete' USING ERRCODE='SKG02'; END IF;
  FOR child_entry IN SELECT value FROM jsonb_array_elements(account->'children') LOOP
   SELECT * INTO owned FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id
    AND o.key_digest=decode(substr(child_entry->'key'->>'digest',8),'hex');
   IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop account has no ownership' USING ERRCODE='SKG02'; END IF;
   SELECT * INTO settled FROM stateknot.child_run_settlements s WHERE s.tenant_id=owned.tenant_id AND s.parent_run_id=owned.parent_run_id AND s.key_digest=owned.key_digest;
   IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop account has no settlement' USING ERRCODE='SKG02'; END IF;
   SELECT * INTO child_terminal FROM stateknot.child_run_terminals t WHERE t.tenant_id=owned.tenant_id AND t.child_run_id=owned.child_run_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop account has no terminal' USING ERRCODE='SKG02'; END IF;
   SELECT * INTO child_admission FROM stateknot.agent_admissions a WHERE a.tenant_id=owned.tenant_id AND a.run_id=owned.child_run_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'registered Join stop account has no child admission' USING ERRCODE='SKG02'; END IF;
   evidence:=convert_from(settled.settlement_bytes,'UTF8')::jsonb;
   terminal:=convert_from(child_terminal.lifecycle_bytes,'UTF8')::jsonb;
   IF (owned.settled AND owned.terminal_pending_at IS NULL AND settled.child_run_id=owned.child_run_id
    AND child_entry->'key'=convert_from(owned.key_bytes,'UTF8')::jsonb
    AND child_entry->>'spawn_digest'='sha256:'||encode(owned.spawn_digest,'hex')
    AND (SELECT array_agg(key ORDER BY key COLLATE "C") FROM jsonb_object_keys(child_entry) key)=ARRAY['child','intent_digest','key','reservation','settlement','spawn_digest']::text[]
    AND child_entry->'reservation'=jsonb_build_object('amount',
     ((convert_from(child_admission.admission_bytes,'UTF8')::jsonb#>'{intent,budget}')-ARRAY['deadline','costs'])
      ||'{"graph_depth":"0","concurrent_branches":"0","fan_out":"0","unpriced_cost_events":"0"}'::jsonb
      ||jsonb_build_object('known_costs',convert_from(child_admission.admission_bytes,'UTF8')::jsonb#>'{intent,budget,costs}'))
    AND child_entry->'settlement'=evidence AND sha256(settled.settlement_bytes)=settled.settlement_digest
    AND child_entry->'child'=convert_from(child_admission.admission_bytes,'UTF8')::jsonb#>'{intent,provenance}'
    AND child_entry->>'intent_digest'='sha256:'||encode(child_admission.intent_digest,'hex')
    AND child_admission.admission_digest=owned.child_admission_digest
    AND evidence=jsonb_build_object('child',child_entry->'child','intent_digest','sha256:'||encode(child_admission.intent_digest,'hex'),
     'admission_digest','sha256:'||encode(child_admission.admission_digest,'hex'),
     'admitted_at',to_char(child_admission.admitted_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
     'terminal',jsonb_build_object('tenant_id',child_terminal.tenant_id,'run_id',child_terminal.child_run_id::text,'sequence',child_terminal.journal_sequence::text,
      'event_id',child_terminal.journal_event_id::text,'recorded_at',to_char(child_terminal.journal_recorded_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),'digest','sha256:'||encode(child_terminal.journal_digest,'hex')),
     'status',terminal->'state'->>'kind',
     'outcome_digest','sha256:'||encode(sha256(convert_to('stateknot.child-run-budget-terminal.v1','UTF8')||decode('00','hex')||child_terminal.lifecycle_bytes),'hex'),
     'usage',CASE terminal->'state'->>'kind' WHEN 'succeeded' THEN terminal#>'{state,result,usage}' WHEN 'failed' THEN terminal#>'{state,failure,usage}' WHEN 'cancelled' THEN terminal#>'{state,cancellation,usage}' END)
    AND evidence->>'status' IN('succeeded','failed','cancelled') AND evidence->'usage'->>'unpriced_cost_events'='0'
    AND terminal->'provenance'=child_entry->'child'
    AND terminal->>'admitted_at'=to_char(child_admission.admitted_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
    AND child_terminal.lifecycle_digest=sha256(child_terminal.lifecycle_bytes)
    AND settled.journal_recorded_at>=child_terminal.journal_recorded_at
    AND settled.journal_sequence<=(wire->'intent'->'observed_head'->>'sequence')::bigint
    AND settled.journal_recorded_at<=(wire->'intent'->'observed_head'->>'recorded_at')::timestamptz
    AND EXISTS (SELECT 1 FROM stateknot.run_events event WHERE event.tenant_id=settled.tenant_id AND event.run_id=settled.parent_run_id AND event.sequence=settled.journal_sequence
     AND event.event_id=settled.journal_event_id AND event.recorded_at=settled.journal_recorded_at AND event.event_digest=settled.journal_digest
     AND event.event_kind='child-run-settled' AND event.source_kind IN('control_plane','worker')
     AND event.projection_digest=sha256(convert_to('["stateknot.child-settlement.v1","sha256:'||encode(settled.key_digest,'hex')||'","sha256:'||encode(settled.settlement_digest,'hex')||'","sha256:'||encode(event.intent_digest,'hex')||'"]','UTF8')))
    AND EXISTS (SELECT 1 FROM stateknot.run_events event WHERE event.tenant_id=child_terminal.tenant_id AND event.run_id=child_terminal.child_run_id AND event.sequence=child_terminal.journal_sequence
     AND event.event_id=child_terminal.journal_event_id AND event.recorded_at=child_terminal.journal_recorded_at AND event.event_digest=child_terminal.journal_digest)
    AND EXISTS (SELECT 1 FROM stateknot.runs child WHERE child.tenant_id=owned.tenant_id AND child.run_id=owned.child_run_id
     AND child.quarantined_at IS NULL AND child.lifecycle_bytes=child_terminal.lifecycle_bytes)
   ) IS NOT TRUE THEN RAISE EXCEPTION 'registered Join stop settlement or account entry mismatch' USING ERRCODE='SKG02'; END IF;
  END LOOP;
  IF EXISTS (SELECT 1 FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id
   AND NOT EXISTS (SELECT 1 FROM jsonb_array_elements(account->'children') entry WHERE entry->'key'=convert_from(o.key_bytes,'UTF8')::jsonb))
  THEN RAISE EXCEPTION 'registered Join stop account omits ownership' USING ERRCODE='SKG02'; END IF;
 END IF;
 SELECT count(*) INTO actual_count FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=whole.tenant_id AND c.run_id=whole.run_id;
 IF actual_count<>whole.frame_count THEN RAISE EXCEPTION 'incomplete closed caller set' USING ERRCODE='SKG02'; END IF;
 FOR position IN 0..whole.frame_count-1 LOOP
  claim:=wire->'intent'->'frames'->position; completion:=wire->'completions'->position;
  IF completion->'start' IS DISTINCT FROM claim->'caller' OR completion->'outcome'->>'kind' IS DISTINCT FROM 'failed'
   OR completion->'outcome'->'failure' IS DISTINCT FROM wire->'intent'->'caller_failure'
   OR completion->'outcome'->'failure'->>'category' IS DISTINCT FROM 'cancelled'
   OR completion->'outcome'->'failure'->'retry_advice'->>'kind' IS DISTINCT FROM 'never'
   OR completion->'outcome'->'failure'->>'caused_by_event_id' IS DISTINCT FROM whole.journal_event_id::text
   OR completion->'usage' IS DISTINCT FROM '{"graph_depth":"0","graph_steps":"0","model_attempts":"0","model_turns":"0","input_tokens":"0","cached_input_tokens":"0","reasoning_tokens":"0","output_tokens":"0","tool_calls":"0","write_calls":"0","remote_agent_delegations":"0","retries":"0","concurrent_branches":"0","fan_out":"0","input_bytes":"0","output_bytes":"0","event_bytes":"0","checkpoint_bytes":"0","artifact_bytes":"0","known_costs":[],"unpriced_cost_events":"0"}'::jsonb
   OR NOT EXISTS (
    SELECT 1 FROM stateknot.graph_frame_closed_callers c
    JOIN stateknot.graph_frame_entries e USING(tenant_id,run_id,graph_namespace)
    JOIN stateknot.graph_frame_heads h USING(tenant_id,run_id,graph_namespace)
    JOIN stateknot.node_attempts n ON n.tenant_id=c.tenant_id AND n.run_id=c.run_id AND n.attempt_id=c.caller_attempt_id
    WHERE c.tenant_id=whole.tenant_id AND c.run_id=whole.run_id AND c.graph_namespace=claim#>>'{checkpoint,frame,namespace}'
     AND e.parent_namespace=previous AND c.entry_digest=e.compound_digest AND c.frame_identity_digest=e.frame_identity_digest
     AND c.entry_digest=decode(substr(claim->>'entry_digest',8),'hex')
     AND c.frame_identity_digest=decode(substr(claim#>>'{checkpoint,frame,digest}',8),'hex')
     AND c.checkpoint_id=h.checkpoint_id AND c.checkpoint_superstep=h.superstep AND c.checkpoint_digest=h.checkpoint_digest AND c.frame_checkpoint_digest=h.frame_checkpoint_digest
     AND c.checkpoint_id=(claim#>>'{checkpoint,checkpoint,checkpoint_id}')::uuid
     AND c.checkpoint_digest=decode(substr(claim#>>'{checkpoint,checkpoint,digest}',8),'hex') AND c.frame_checkpoint_digest=decode(substr(claim#>>'{checkpoint,digest}',8),'hex')
     AND c.caller_attempt_id=(claim->'caller'->>'attempt_id')::uuid AND c.caller_start_digest=n.start_digest
     AND c.caller_start_digest=decode(substr(claim->'caller'->>'digest',8),'hex') AND c.caller_compound_digest=decode(substr(claim->>'caller_binding_digest',8),'hex')
     AND n.graph_namespace=e.parent_namespace AND n.base_checkpoint_id=e.parent_checkpoint_id AND n.start_digest=c.caller_start_digest
     AND ((n.attempt_id=e.caller_attempt_id AND c.caller_compound_digest=e.compound_digest
       AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace))
      OR EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=e.tenant_id AND b.run_id=e.run_id AND b.graph_namespace=e.graph_namespace
       AND b.caller_attempt_id=n.attempt_id AND b.caller_start_digest=n.start_digest AND b.compound_digest=c.caller_compound_digest
       AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings later WHERE later.tenant_id=b.tenant_id AND later.run_id=b.run_id AND later.graph_namespace=b.graph_namespace AND later.worker_epoch>b.worker_epoch)))
     AND convert_from(c.completion_bytes,'UTF8')::jsonb=completion AND c.completion_digest=decode(substr(completion->>'digest',8),'hex')
     AND c.journal_sequence=whole.journal_sequence AND c.journal_event_id=whole.journal_event_id AND c.journal_recorded_at=whole.journal_recorded_at AND c.journal_digest=whole.journal_digest
     AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions ordinary WHERE ordinary.tenant_id=n.tenant_id AND ordinary.run_id=n.run_id AND ordinary.attempt_id=n.attempt_id)
     AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns returned WHERE returned.tenant_id=e.tenant_id AND returned.run_id=e.run_id AND returned.graph_namespace=e.graph_namespace)
   ) THEN RAISE EXCEPTION 'incomplete whole closed caller component' USING ERRCODE='SKG02'; END IF;
  previous:=claim#>>'{checkpoint,frame,namespace}';
 END LOOP;
 IF previous<>whole.active_namespace OR EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=whole.tenant_id AND e.run_id=whole.run_id
  AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_returns x WHERE x.tenant_id=e.tenant_id AND x.run_id=e.run_id AND x.graph_namespace=e.graph_namespace)
  AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_closed_callers c WHERE c.tenant_id=e.tenant_id AND c.run_id=e.run_id AND c.graph_namespace=e.graph_namespace))
  OR EXISTS(SELECT 1 FROM stateknot.tool_invocations t WHERE t.tenant_id=whole.tenant_id AND t.run_id=whole.run_id AND t.current_status NOT IN('committed','failed'))
  OR EXISTS(SELECT 1 FROM stateknot.model_invocations m WHERE m.tenant_id=whole.tenant_id AND m.run_id=whole.run_id AND m.current_status NOT IN('committed','failed'))
  OR EXISTS(SELECT 1 FROM stateknot.child_run_ownership o WHERE o.tenant_id=whole.tenant_id AND o.parent_run_id=whole.run_id AND NOT o.settled)
  OR EXISTS(SELECT 1 FROM stateknot.node_attempts n WHERE n.tenant_id=whole.tenant_id AND n.run_id=whole.run_id
   AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempts later WHERE later.tenant_id=n.tenant_id AND later.run_id=n.run_id AND later.activation_digest=n.activation_digest AND later.journal_sequence>n.journal_sequence)
   AND NOT EXISTS(SELECT 1 FROM stateknot.node_attempt_completions c WHERE c.tenant_id=n.tenant_id AND c.run_id=n.run_id AND c.attempt_id=n.attempt_id)
   AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_entries e WHERE e.tenant_id=n.tenant_id AND e.run_id=n.run_id AND e.caller_attempt_id=n.attempt_id)
   AND NOT EXISTS(SELECT 1 FROM stateknot.graph_frame_caller_bindings b WHERE b.tenant_id=n.tenant_id AND b.run_id=n.run_id AND b.caller_attempt_id=n.attempt_id)
   AND n.attempt_id IS DISTINCT FROM stopped_attempt)
 THEN RAISE EXCEPTION 'whole closure cannot erase open frames or unsettled work' USING ERRCODE='SKG02'; END IF;
 RETURN NULL;
END $$;
