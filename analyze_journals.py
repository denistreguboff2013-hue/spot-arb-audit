"""Read-only, standard library. Run from extracted archive: python3 analyze_journals.py"""
import json,pathlib,collections
root=pathlib.Path(__file__).resolve().parent/'evidence'
for run,tid,ticket in [('R1410I',2414173,765),('R1410J',2416013,9679)]:
    dirs=list((root/run).glob('*.journal'))
    if not dirs: print(run,'no journal');continue
    d=dirs[0]; totals=collections.Counter()
    for f in sorted(d.glob('ucm-*.jsonl')):
        with f.open() as r:
            h=json.loads(next(r));rows=[json.loads(l) for l in r]
        assert len(rows)==h['retained'] and h['total']==h['retained']+h['overwritten']
        assert all(b['ticket']==a['ticket']+1 for a,b in zip(rows,rows[1:]))
        totals.update({k:h[k] for k in ['total','retained','overwritten']})
        if h['tid']==tid:
            x=next((v for v in rows if v['ticket']==ticket),None)
            if not x: print(run,'fault missing');continue
            print(run,'fault', {k:v for k,v in x.items() if k!='stamps'})
            t=x['enqueued_ns']
            for name,s in zip(h['stamps'],x['stamps']):
                if s[0]:print(name,'relative_ms',round((s[0]-t)/1e6,6),'cpu_ms',round(s[1]/1e6,6),'bracket_us',round((s[2]-s[0])/1000,3) if len(s)==3 else None)
    print(run,'UCM totals',dict(totals))
    with (d/'consumer.jsonl').open() as r:
        h=json.loads(next(r));rows=[json.loads(l) for l in r]
    print('consumer',h['total'],h['retained'],h['overwritten'],dict(collections.Counter(x['outcome'] for x in rows)))
    print('missing upstream',[(n,sum(not x['upstream'][i] for x in rows)) for i,n in enumerate(h['upstream'])])
