"""Chronological evaluation helpers for V1/V2 benchmarking."""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable
import pandas as pd
from .models import PullbackSignal

@dataclass(frozen=True)
class ChronologicalSplit:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame

@dataclass(frozen=True)
class SignalOutcomeMetrics:
    signal_count: int
    target_1_hits: int
    target_2_hits: int
    invalidations: int
    continuation_count: int
    average_r: float
    expectancy_r: float
    average_mfe_r: float
    average_mae_r: float
    average_time_to_target_bars: float | None
    average_time_to_invalidation_bars: float | None


def chronological_split(candles: pd.DataFrame, train_ratio=.60, validation_ratio=.20) -> ChronologicalSplit:
    if train_ratio <= 0 or validation_ratio <= 0 or train_ratio + validation_ratio >= 1: raise ValueError("invalid chronological split ratios")
    data=candles.sort_values("timestamp").reset_index(drop=True); n=len(data); a=int(n*train_ratio); b=int(n*(train_ratio+validation_ratio))
    return ChronologicalSplit(data.iloc[:a].copy(),data.iloc[a:b].copy(),data.iloc[b:].copy())


def evaluate_signals(signals: Iterable[PullbackSignal], candles: pd.DataFrame, max_horizon_bars=60) -> SignalOutcomeMetrics:
    signals=list(signals); data=candles.sort_values("timestamp").reset_index(drop=True)
    target1=target2=invalid=continuation=0; rs=[]; mfes=[]; maes=[]; t1=[]; invtimes=[]
    for signal in signals:
        rows=data[data["timestamp"] > signal.timestamp].head(max_horizon_bars)
        if rows.empty: continue
        continuation += 1; r=abs(signal.trigger_price-signal.invalidation_level)
        if r == 0: continue
        mfe=Decimal("0"); mae=Decimal("0"); t1_hit=t2_hit=False; invalidated=False; outcome=None; close=signal.trigger_price
        for j,row in enumerate(rows,1):
            high=Decimal(str(row["high"])); low=Decimal(str(row["low"])); close=Decimal(str(row["close"]))
            if signal.direction=="LONG":
                mfe=max(mfe,high-signal.trigger_price); mae=max(mae,signal.trigger_price-low)
                if not t1_hit and high>=signal.trigger_price+r: t1_hit=True; target1+=1; t1.append(j)
                if not t2_hit and high>=signal.trigger_price+2*r: t2_hit=True; target2+=1
                if low<=signal.invalidation_level and not t2_hit: invalidated=True; invalid+=1; invtimes.append(j); outcome="INVALIDATION"; break
            else:
                mfe=max(mfe,signal.trigger_price-low); mae=max(mae,high-signal.trigger_price)
                if not t1_hit and low<=signal.trigger_price-r: t1_hit=True; target1+=1; t1.append(j)
                if not t2_hit and low<=signal.trigger_price-2*r: t2_hit=True; target2+=1
                if high>=signal.invalidation_level and not t2_hit: invalidated=True; invalid+=1; invtimes.append(j); outcome="INVALIDATION"; break
            if t2_hit: outcome="T2"; break
            if t1_hit and outcome is None: outcome="T1"
        if outcome=="T2": rs.append(2.0)
        elif outcome=="T1": rs.append(1.0)
        elif invalidated: rs.append(-1.0)
        else: rs.append(float((close-signal.trigger_price)/r)*(1 if signal.direction=="LONG" else -1))
        mfes.append(float(mfe/r)); maes.append(float(mae/r))
    avg_r=sum(rs)/len(rs) if rs else 0.0
    return SignalOutcomeMetrics(len(signals),target1,target2,invalid,continuation,avg_r,avg_r,sum(mfes)/len(mfes) if mfes else 0.0,sum(maes)/len(maes) if maes else 0.0,sum(t1)/len(t1) if t1 else None,sum(invtimes)/len(invtimes) if invtimes else None)
