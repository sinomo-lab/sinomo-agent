#!/usr/bin/env python3
"""Exact JSON subtree bytes from compact Rust serde observations.

No normalization/re-encoding is performed. UTF-8 validation, duplicate field
rejection and independent prepared-payload digest checking precede use.
"""
from __future__ import annotations
import importlib.util
import json
from pathlib import Path
from typing import Any

SPEC=importlib.util.spec_from_file_location('native_projector',Path(__file__).with_name('shard-native-projector.py'))
PROJECTOR=importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROJECTOR)
Rejected=PROJECTOR.Rejected
DECODER=json.JSONDecoder()


def _space(text:str,index:int)->int:
    while index<len(text) and text[index] in ' \t\r\n':index+=1
    return index


def subtree(raw:bytes,path:list[str|int])->bytes:
    # First validate the complete input losslessly, including duplicate keys.
    PROJECTOR.exact_json(raw)
    text=raw.decode('utf-8')
    start,end=0,len(text)
    for part in path:
        start=_space(text,start)
        if isinstance(part,str):
            if text[start]!='{':raise Rejected('JSON subtree path expects object')
            cursor=_space(text,start+1);found=None
            while cursor<end and text[cursor]!='}':
                key,stop=DECODER.raw_decode(text,cursor)
                if not isinstance(key,str):raise Rejected('JSON key is not text')
                cursor=_space(text,stop)
                if text[cursor]!=':':raise Rejected('JSON colon missing')
                begin=_space(text,cursor+1)
                _,finish=DECODER.raw_decode(text,begin)
                if key==part:found=(begin,finish)
                cursor=_space(text,finish)
                if text[cursor]==',':cursor=_space(text,cursor+1)
                elif text[cursor]!='}':raise Rejected('JSON object separator missing')
            if found is None:raise Rejected('JSON subtree field absent')
            start,end=found
        elif type(part) is int and part>=0:
            if text[start]!='[':raise Rejected('JSON subtree path expects array')
            cursor=_space(text,start+1);index=0;found=None
            while cursor<end and text[cursor]!=']':
                begin=cursor;_,finish=DECODER.raw_decode(text,begin)
                if index==part:found=(begin,finish)
                index+=1;cursor=_space(text,finish)
                if text[cursor]==',':cursor=_space(text,cursor+1)
                elif text[cursor]!=']':raise Rejected('JSON array separator missing')
            if found is None:raise Rejected('JSON subtree array index absent')
            start,end=found
        else:raise Rejected('JSON subtree path component invalid')
    return text[start:end].encode('utf-8')


def canonical_prepared_payload(raw:bytes,path:list[str|int],digest:str)->bytes:
    encoded=subtree(raw,path)
    expected='dfs-prepared-operation-v7:blake3:'+PROJECTOR.blake3(b'sinomo.dfs.prepared-operation\0v7\0'+encoded)
    if digest!=expected:raise Rejected('exact observed payload bytes differ from native digest')
    return encoded


def self_test()->None:
    raw=b'{"entry":{"record":{"payload":{"z":18446744073709551615,"x":["\\\"",{"nested":"native"}]}}}}'
    expected=b'{"z":18446744073709551615,"x":["\\\"",{"nested":"native"}]}'
    assert subtree(raw,['entry','record','payload'])==expected
    digest='dfs-prepared-operation-v7:blake3:'+PROJECTOR.blake3(b'sinomo.dfs.prepared-operation\0v7\0'+expected)
    assert canonical_prepared_payload(raw,['entry','record','payload'],digest)==expected
    assert subtree(raw,['entry','record','payload','x',1])==b'{"nested":"native"}'
    for value,path in [(b'{"x":1,"x":2}',['x']),(raw,['absent']),(raw,['entry',0])]:
        try:subtree(value,path)
        except (Rejected,KeyError,IndexError):pass
        else:raise AssertionError('invalid raw path accepted')
    try:canonical_prepared_payload(raw,['entry','record','payload'],digest[:-1]+'0')
    except Rejected:pass
    else:raise AssertionError('changed digest accepted')
    print('PASS exact UTF-8 subtree/u64/escape/array + digest/tamper controls; synthetic codec checks only')


if __name__=='__main__':self_test()
