import json,os,pathlib,shutil,signal,subprocess,sys,time
from datetime import datetime,timezone
h=pathlib.Path(__file__).parent
binary,output=sys.argv[1:]; output=pathlib.Path(output)
def safe():
    assert json.load(open('/home/tmpadmin/spotbot/data/real_kill_switch.json'))['state']=='ENGAGED'
    assert any(l.strip()=='autonomous_real_enabled: false' for l in open('/home/tmpadmin/spotbot/config.yaml'))
    for name in ['spotbot.service','spotarb-native-paper-continuous.service']:
        assert subprocess.run(['systemctl','is-active',name],capture_output=True,text=True).stdout.strip()=='inactive',name
safe()
start=time.monotonic(); reason=None
with (h/'R1410F_PUBLIC.log').open('xb') as log,(h/'R1410F_MONITOR.jsonl').open('x',buffering=1) as mon:
    p=subprocess.Popen([binary,'3600',str(output)],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    (h/'R1410F_RUN.pid').write_text(str(p.pid)+'\n')
    print('R1410F live process PID',p.pid,flush=True)
    try:
        while p.poll() is None:
            try:
                safe()
                mem=int(next(l.split()[1] for l in open('/proc/meminfo') if l.startswith('MemAvailable:')))
                stat=pathlib.Path(f'/proc/{p.pid}/status').read_text()
                rss=int(next(l.split()[1] for l in stat.splitlines() if l.startswith('VmRSS:')))
                ticks=pathlib.Path(f'/proc/{p.pid}/stat').read_text().split()
                cpu_ticks=int(ticks[13])+int(ticks[14])
                lines=[l.split() for l in subprocess.run(['ss','-tnp'],capture_output=True,text=True).stdout.splitlines() if f'pid={p.pid},' in l]
                established=[l for l in lines if l[0]=='ESTAB']
                q=[int(l[1]) for l in established]
                disk=min(shutil.disk_usage('/mnt/spotarb_db_test').free,shutil.disk_usage('/mnt/spotarb_disk2').free)
                row=dict(utc=datetime.now(timezone.utc).isoformat(),elapsed_s=round(time.monotonic()-start,2),rss_kib=rss,available_kib=mem,cpu_ticks=cpu_ticks,established=len(established),recvq_sum=sum(q),recvq_max=max(q,default=0),free_bytes=disk)
                mon.write(json.dumps(row)+'\n')
                if rss>3*1024*1024 or mem<600*1024 or disk<700*1024*1024 or log.tell()>128*1024*1024:reason='resource safety threshold';break
                if time.monotonic()-start>3660:reason='hard deadline';break
            except Exception as e:
                if p.poll() is None:reason=f'monitor safety: {e!r}'
                break
            time.sleep(5)
    finally:
        if p.poll() is None:
            os.killpg(p.pid,signal.SIGTERM)
            try:p.wait(timeout=8)
            except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);p.wait()
        rc=p.wait()
        result=dict(exit=rc,monitor_stop=reason,elapsed_s=round(time.monotonic()-start,2),report_exists=output.exists(),orders_capability=False)
        (h/'R1410F_PUBLIC.exit').write_text(str(rc)+'\n')
        (h/'R1410F_MONITOR_RESULT.json').write_text(json.dumps(result)+'\n')
        print('R1410F_FINAL',json.dumps(result),flush=True)
