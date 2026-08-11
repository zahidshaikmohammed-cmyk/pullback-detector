"Read-only browser dashboard."
from __future__ import annotations
import csv,json,threading
from dataclasses import asdict
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from .lifecycle import PullbackLifecycleEngine
from .live import LIVE_ANATOMY

def _j(v:Any):
    if isinstance(v,datetime):return v.astimezone(timezone.utc).isoformat()
    if isinstance(v,Decimal):return str(v)
    return v
def _tail(p,n=5000):
    if not p.exists():return []
    try:
        with p.open(encoding="utf-8") as f:return f.readlines()[-n:]
    except OSError:return []
class DashboardData:
    def __init__(self,root,state):self.root=Path(root);self.state=state;self.lock=threading.Lock();self.cache=None
    def _dt(self,v):
        try:return datetime.fromisoformat(v.replace("Z","+00:00")).astimezone(timezone.utc) if v else None
        except(ValueError,TypeError):return None
    def _universe(self):
        p=self.root/"universe.csv"
        try:
            with p.open(encoding="utf-8",newline="") as f:return list(csv.DictReader(f))
        except(OSError,csv.Error):return []
    def _life(self):
        s=PullbackLifecycleEngine(self.root).snapshot()
        def c(x):
            d=asdict(x);d["snapshot"]=asdict(x.snapshot);return json.loads(json.dumps(d,default=_j))
        return [c(x) for x in s["active"]],[c(x) for x in s["closed"]]
    def snapshot(self):
        now=datetime.now(timezone.utc);u=self._universe();files=[self.root/x/f"{now:%Y-%m-%d}.jsonl" for x in("normalized","candles","signals")];life=self.root/"setup_events.jsonl"
        key=tuple(p.stat().st_mtime if p.exists() else 0 for p in files+[life])+(str(self.state.get("status")),str(self.state.get("last_error")),len(LIVE_ANATOMY))
        with self.lock:
            if self.cache and self.cache[0]==key:return self.cache[1]
            ticks={};cs={};hist={};sigs=[]
            for l in _tail(files[0]):
                try:x=json.loads(l);ticks[str(x.get("instrument_id"))]=x
                except:pass
            for l in _tail(files[1],12000):
                try:x=json.loads(l)
                except:continue
                if not x.get("complete"):continue
                sid=str(x.get("instrument_id"));tf=str(x.get("timeframe_seconds",300));cs.setdefault(sid,{})[tf]=x
                if tf=="300":hist.setdefault(sid,[]).append(x)
            for k in hist:hist[k]=sorted(hist[k],key=lambda x:x.get("end",""))[-36:]
            for l in _tail(files[2],1000):
                try:sigs.append(json.loads(l))
                except:pass
            sigs.sort(key=lambda x:x.get("timestamp",""),reverse=True);a,closed=self._life();active={str(x["snapshot"]["instrument_id"]) for x in a};ins=[]
            for r in u:
                sid=str(r.get("security_id"));t=ticks.get(sid,{});an=dict(LIVE_ANATOMY.get(int(sid),{})) if sid.isdigit() else {};ins.append({"security_id":r.get("security_id"),"symbol":r.get("symbol") or r.get("trading_symbol") or sid,"latest_price":t.get("price"),"receive_timestamp":t.get("received_at"),"candle_5m_history":hist.get(sid,[]),"anatomy":an,"state":"ACTIVE" if sid in active else an.get("state") or an.get("status") or an.get("phase") or "WATCHING"})
            h=dict(self.state.get("last_report") or {});h["service_status"]=self.state.get("status",h.get("service_status"));h["last_error"]=self.state.get("last_error");lr=self._dt(h.get("last_receive_timestamp"));h["feed_connected"]=bool(h.get("dhan_connection_status") in("connected","connected_after_reconnect") and lr and(now-lr).total_seconds()<=60)
            rej=[]
            for r in u:
                sid=str(r.get("security_id"))
                for l in _tail(self.root/f"pullback_candidates_{sid}.jsonl",200):
                    try:x=json.loads(l)
                    except:continue
                    if x.get("event")=="candidate_rejected":x["symbol"]=r.get("symbol") or sid;rej.append(x)
            p={"generated_at":now.isoformat(),"health":h,"instruments":ins,"active_setups":a,"recently_closed_setups":closed[:50],"rejections":sorted(rej,key=lambda x:x.get("timestamp",""),reverse=True)[:80],"active_signals":sigs[:25]}
            self.cache=(key,p);return p
HTML="".join(p.read_text(encoding="utf-8") for p in (Path(__file__).with_name("dashboard_head1.html"),Path(__file__).with_name("dashboard_theme.html"),Path(__file__).with_name("dashboard_head2.html"),Path(__file__).with_name("dashboard_script1.html"),Path(__file__).with_name("dashboard_script2.html")))
