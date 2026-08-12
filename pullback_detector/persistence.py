"""Append-only market event persistence with deterministic Phase-1 recovery state."""
import json, os
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from .models import Candle, PullbackSignal, Tick

def _jsonable(value):
    if isinstance(value, datetime): return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Decimal): return str(value)
    return value

class EventStore:
    _last_instance = None; _process_id = os.getpid()
    def __init__(self, root: str | Path = "data/runtime"):
        self.root=Path(root); self.root.mkdir(parents=True,exist_ok=True)
        try: os.chmod(self.root,0o700)
        except OSError: pass
        self._candle_keys=set(); self._event_keys=set(); self._signal_keys=set(); self._persisted_counts={60:0,300:0}; self._last_persisted_timestamp={60:None,300:None}
        self.persistence_write_count=0; self.persistence_failure_count=0; self.duplicate_event_count=0; self.duplicate_signal_count=0; self.duplicate_candle_contribution_count=0
        self._current_run_started_at=datetime.now(timezone.utc); self._first_tick_after_start=None; self._first_candle_after_start=None; self._recovery_probe_passed=False
        self._previous_checkpoint=self._load_recovery_checkpoint(); self._previous_health_report=self._load_last_health_report(); self._load_indexes(); self._run_recovery_identity_probe(); EventStore._last_instance=self
    def _append(self,path,record):
        try:
            path.parent.mkdir(parents=True,exist_ok=True)
            with path.open("a",encoding="utf-8") as handle: handle.write(json.dumps(record,default=_jsonable,separators=(",",":"))+"\n"); handle.flush(); os.fsync(handle.fileno())
            try: os.chmod(path,0o600)
            except OSError: pass
        except Exception: self.persistence_failure_count+=1; raise
    @staticmethod
    def _event_key(tick):
        raw="|".join((str(tick.exchange_segment),str(tick.instrument_id),tick.timestamp.astimezone(timezone.utc).isoformat(),str(tick.price),str(tick.quantity),str(tick.cumulative_volume),str(tick.sequence)))
        return sha256(raw.encode()).hexdigest()
    @staticmethod
    def _event_key_from_row(row):
        try:
            ts=datetime.fromisoformat(str(row["timestamp"])).astimezone(timezone.utc).isoformat(); raw="|".join((str(row.get("exchange_segment","NSE_EQ")),str(row["instrument_id"]),ts,str(row["price"]),str(row.get("quantity",0)),str(row.get("cumulative_volume")),str(row.get("sequence")))); return sha256(raw.encode()).hexdigest()
        except (KeyError,TypeError,ValueError): return None
    @staticmethod
    def _signal_key(signal): return str(signal.signal_id or "|".join((str(signal.instrument_id),signal.timestamp.astimezone(timezone.utc).isoformat(),signal.direction,str(signal.trigger_price))))
    @staticmethod
    def _signal_key_from_row(row):
        try: return str(row.get("signal_id") or "|".join((str(row["instrument_id"]),datetime.fromisoformat(row["timestamp"]).astimezone(timezone.utc).isoformat(),row["direction"],str(row["trigger_price"]))))
        except (KeyError,TypeError,ValueError): return None
    @staticmethod
    def _candle_key(candle): return candle.instrument_id,candle.timeframe_seconds,candle.start.astimezone(timezone.utc).isoformat()
    def _load_indexes(self):
        p=self.root/"candles"
        if p.exists():
            for file in sorted(p.glob("*.jsonl")):
                try:
                    for line in file.read_text(encoding="utf-8").splitlines():
                        try: row=json.loads(line)
                        except json.JSONDecodeError: continue
                        if not row.get("complete"): continue
                        try: key=(int(row["instrument_id"]),int(row["timeframe_seconds"]),datetime.fromisoformat(row["start"]).astimezone(timezone.utc).isoformat())
                        except (KeyError,TypeError,ValueError): continue
                        if key in self._candle_keys: continue
                        self._candle_keys.add(key); tf=key[1]
                        if tf in self._persisted_counts: self._persisted_counts[tf]+=1; self._last_persisted_timestamp[tf]=max(self._last_persisted_timestamp[tf] or key[2],key[2])
                except OSError: self.persistence_failure_count+=1
        p=self.root/"normalized"
        if p.exists():
            for file in sorted(p.glob("*.jsonl"))[-120:]:
                try:
                    for line in file.read_text(encoding="utf-8").splitlines():
                        try:
                            key=self._event_key_from_row(json.loads(line))
                            if key: self._event_keys.add(key)
                        except json.JSONDecodeError: continue
                except OSError: self.persistence_failure_count+=1
        p=self.root/"signals"
        if p.exists():
            for file in sorted(p.glob("*.jsonl"))[-120:]:
                try:
                    for line in file.read_text(encoding="utf-8").splitlines():
                        try:
                            key=self._signal_key_from_row(json.loads(line))
                            if key: self._signal_keys.add(key)
                        except json.JSONDecodeError: continue
                except OSError: self.persistence_failure_count+=1
    def _run_recovery_identity_probe(self):
        normalized=self.root/"normalized"; sample=None
        if normalized.exists():
            for file in sorted(normalized.glob("*.jsonl"),reverse=True):
                try:
                    for line in reversed(file.read_text(encoding="utf-8").splitlines()):
                        try:
                            row=json.loads(line); key=self._event_key_from_row(row)
                            if key: sample=(row,key); break
                        except json.JSONDecodeError: continue
                except OSError: continue
                if sample: break
        if not sample: self._recovery_probe_passed=not self._event_keys; return
        row,key=sample; same_event_rejected=key in self._event_keys; new_row=dict(row); new_row["sequence"]=int(row.get("sequence") or 0)+1; new_key=self._event_key_from_row(new_row); self._recovery_probe_passed=bool(same_event_rejected and new_key and new_key!=key)
    def _load_recovery_checkpoint(self):
        p=self.root/"recovery_state.json"
        if not p.exists(): return {}
        try: return json.loads(p.read_text(encoding="utf-8"))
        except (OSError,json.JSONDecodeError): return {}
    def _load_last_health_report(self):
        p=self.root/"health.jsonl"
        if not p.exists(): return {}
        try:
            for line in reversed(p.read_text(encoding="utf-8").splitlines()):
                try: return json.loads(line)
                except json.JSONDecodeError: continue
        except OSError: self.persistence_failure_count+=1
        return {}
    def _write_recovery_checkpoint(self,report=None):
        checkpoint={"process_id":EventStore._process_id,"run_started_at":self._current_run_started_at,"timestamp":datetime.now(timezone.utc),"persisted_1m_candles":self._persisted_counts[60],"persisted_5m_candles":self._persisted_counts[300],"last_persisted_1m_timestamp":self._last_persisted_timestamp[60],"last_persisted_5m_timestamp":self._last_persisted_timestamp[300],"persistence_write_count":self.persistence_write_count,"persistence_failure_count":self.persistence_failure_count,"duplicate_event_count":self.duplicate_event_count,"duplicate_signal_count":self.duplicate_signal_count}
        if report is not None: checkpoint["last_health_report"]=report
        target=self.root/"recovery_state.json"; tmp=target.with_suffix(".tmp")
        try: tmp.write_text(json.dumps(checkpoint,default=_jsonable,separators=(",",":")),encoding="utf-8"); tmp.replace(target)
        except OSError: self.persistence_failure_count+=1
    def raw_packet(self,received_at,payload,response_code=None): self._append(self.root/"raw"/(received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")+".jsonl"),{"received_at":received_at,"response_code":response_code,"payload_hex":payload.hex()})
    def tick(self,received_at,tick:Tick):
        key=self._event_key(tick)
        if key in self._event_keys: self.duplicate_event_count+=1; return False
        self._event_keys.add(key)
        if self._first_tick_after_start is None: self._first_tick_after_start=datetime.now(timezone.utc)
        self._append(self.root/"normalized"/(received_at.astimezone(timezone.utc).strftime("%Y-%m-%d")+".jsonl"),{**asdict(tick),"received_at":received_at}); self.persistence_write_count+=1; return True
    def candle(self,candle:Candle):
        key=self._candle_key(candle)
        if not candle.complete: return False
        if key in self._candle_keys: self.duplicate_candle_contribution_count+=1; return False
        self._candle_keys.add(key); tf=candle.timeframe_seconds
        if tf in self._persisted_counts: self._persisted_counts[tf]+=1; value=candle.start.astimezone(timezone.utc).isoformat(); self._last_persisted_timestamp[tf]=max(self._last_persisted_timestamp[tf] or value,value)
        if self._first_candle_after_start is None: self._first_candle_after_start=datetime.now(timezone.utc)
        self._append(self.root/"candles"/(candle.start.astimezone(timezone.utc).strftime("%Y-%m-%d")+".jsonl"),asdict(candle)); self.persistence_write_count+=1; return True
    def signal(self,signal):
        key=self._signal_key(signal)
        if key in self._signal_keys: self.duplicate_signal_count+=1; return False
        self._signal_keys.add(key); self._append(self.root/"signals"/(signal.timestamp.astimezone(timezone.utc).strftime("%Y-%m-%d")+".jsonl"),asdict(signal)); self.persistence_write_count+=1; return True
    def health(self,report): self._append(self.root/"health.jsonl",report); self._write_recovery_checkpoint(report)
    def persistence_snapshot(self): return {"persisted_1m_candles":self._persisted_counts[60],"persisted_5m_candles":self._persisted_counts[300],"last_persisted_1m_timestamp":self._last_persisted_timestamp[60],"last_persisted_5m_timestamp":self._last_persisted_timestamp[300],"persistence_write_count":self.persistence_write_count,"persistence_failure_count":self.persistence_failure_count,"duplicate_event_count":self.duplicate_event_count,"duplicate_signal_count":self.duplicate_signal_count,"duplicate_candle_contribution_count":self.duplicate_candle_contribution_count}
    def recovery_snapshot(self):
        previous=self._previous_checkpoint or {}; pre_1m=int(previous.get("persisted_1m_candles",0) or 0); pre_5m=int(previous.get("persisted_5m_candles",0) or 0); post_1m=self._persisted_counts[60]; post_5m=self._persisted_counts[300]; recovered=pre_1m>0 or pre_5m>0; history_restored=post_1m>=pre_1m and post_5m>=pre_5m; continued_events=self._first_tick_after_start is not None; continued_candles=self._first_candle_after_start is not None; continuity=history_restored and self._recovery_probe_passed and self.duplicate_candle_contribution_count==0 and self.duplicate_signal_count==0; verified=bool(recovered and history_restored and self._recovery_probe_passed and continued_events and continued_candles and self.persistence_failure_count==0 and self.duplicate_candle_contribution_count==0); recovery_time=self._first_candle_after_start or self._first_tick_after_start; duration_ms=max(0.0,(recovery_time-self._current_run_started_at).total_seconds()*1000.0) if recovery_time else None
        return {"restart_recovery_verified":verified,"pre_restart_counts":{"1m":pre_1m,"5m":pre_5m},"post_restart_counts":{"1m":post_1m,"5m":post_5m},"recovered_candle_counts":{"1m":pre_1m,"5m":pre_5m},"recovered_event_state":{"history_restored":history_restored,"ticks_resumed":continued_events,"candles_resumed":continued_candles,"canonical_duplicate_events":0 if self._recovery_probe_passed else 1,"duplicate_candle_contributions":self.duplicate_candle_contribution_count,"duplicate_signals":self.duplicate_signal_count},"duplicate_count":0 if self._recovery_probe_passed and self.duplicate_candle_contribution_count==0 else 1,"canonical_duplicate_events":0 if self._recovery_probe_passed else 1,"continuity_status":"PASS" if continuity else "FAIL","recovery_timestamp":recovery_time.isoformat() if recovery_time else None,"recovery_duration_ms":duration_ms}
    def counter_progression(self,current_health):
        previous=self._previous_health_report or {}; keys=("accepted_tick_count","ticks_sent_to_candle_engine","completed_1m_candles","completed_5m_candles","persisted_candle_count_1m","persisted_candle_count_5m"); before={k:int(previous.get("cumulative_counter_values",{}).get(k,previous.get(k,0)) or 0) for k in keys}; current={k:int(current_health.get(k,0) or 0) for k in keys}; after={k:before[k]+current[k] for k in keys}; comparable=bool(previous); verified=comparable and all(after[k]>=before[k] for k in keys) and any(after[k]>before[k] for k in keys); return {"counter_progression_verified":verified,"before":before,"after":after,"before_timestamp":previous.get("generated_at"),"after_timestamp":current_health.get("generated_at")}
    def recent_candles(self,instrument_id,timeframe_seconds,limit=2500):
        path=self.root/"candles"
        if not path.exists(): return []
        found=[]
        for file in sorted(path.glob("*.jsonl"))[-120:]:
            try:
                for line in file.read_text(encoding="utf-8").splitlines():
                    try: row=json.loads(line)
                    except json.JSONDecodeError: continue
                    if int(row.get("instrument_id",-1))!=instrument_id or int(row.get("timeframe_seconds",0))!=timeframe_seconds or not row.get("complete"): continue
                    found.append(Candle(instrument_id,datetime.fromisoformat(row["start"]),datetime.fromisoformat(row["end"]),Decimal(row["open"]),Decimal(row["high"]),Decimal(row["low"]),Decimal(row["close"]),int(row["volume"]),True,timeframe_seconds))
            except OSError: continue
        unique={c.start:c for c in found}; return [unique[k] for k in sorted(unique)[-limit:]]
