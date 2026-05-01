import boto3
import json
import urllib.request
import urllib.error
import ssl
from datetime import datetime

_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

ec2_client = boto3.client('ec2', region_name='ap-northeast-1')
rds_client = boto3.client('rds', region_name='ap-northeast-1')
ssm_client = boto3.client('ssm', region_name='ap-northeast-1')

EC2_INSTANCE_ID   = 'i-014c42b58e2f8cc69'
RDS_INSTANCE_ID   = 'tododb'
HTTP_FAIL_THRESH  = 5   # HTTP: 5回連続NGでリブート
EC2_FAIL_THRESH   = 3   # EC2ステータスチェックNG: 3回でリブート


HTTP_CHECKS = [
    ("nginx",   "http://57.182.38.255/stock.html",              8),
    ("stock",   "http://57.182.38.255/api/stock/candles?symbol=7203.T&interval=1d", 10),
    ("calendar","http://57.182.38.255/api/calendar/events",  10),
]


# ===== SSM ヘルパー =====

def get_param(key, default='0'):
    try:
        return ssm_client.get_parameter(Name=f'/golfspace-monitor/{key}')['Parameter']['Value']
    except:
        return default

def set_param(key, val):
    ssm_client.put_parameter(
        Name=f'/golfspace-monitor/{key}',
        Value=str(val), Type='String', Overwrite=True
    )

def get_count(key):
    return int(get_param(key, '0'))

def set_count(key, val):
    set_param(key, val)


# ===== EC2 状態取得 =====

def get_ec2_state():
    try:
        resp = ec2_client.describe_instances(InstanceIds=[EC2_INSTANCE_ID])
        return resp['Reservations'][0]['Instances'][0]['State']['Name']
    except:
        return 'unknown'

def get_ec2_status():
    """EC2ステータスチェック（inst_status, sys_status）"""
    try:
        resp = ec2_client.describe_instance_status(
            InstanceIds=[EC2_INSTANCE_ID], IncludeAllInstances=True)
        if not resp['InstanceStatuses']:
            return False, 'no status'
        s = resp['InstanceStatuses'][0]
        state      = s['InstanceState']['Name']
        inst_st    = s['InstanceStatus']['Status']
        sys_st     = s['SystemStatus']['Status']
        ok = (state == 'running' and inst_st == 'ok' and sys_st == 'ok')
        return ok, f"state={state} inst={inst_st} sys={sys_st}"
    except Exception as e:
        return False, str(e)


# ===== HTTP チェック =====

def check_http(name, url, timeout):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "golfspace-monitor/1.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            return True, f"http={resp.status}"
    except urllib.error.HTTPError as e:
        return True, f"http={e.code}"   # 4xx/5xx もサーバー応答あり → OK
    except Exception as e:
        return False, str(e)


# ===== RDS チェック =====

def check_rds():
    try:
        resp = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_ID)
        status = resp['DBInstances'][0]['DBInstanceStatus']
        return status == 'available', f"status={status}"
    except Exception as e:
        return False, str(e)


# ===== EC2 再起動（非同期・ループ防止付き） =====

def trigger_ec2_restart(reason):
    state = get_ec2_state()
    now   = datetime.now()
    if state in ('stopping', 'pending'):
        # 既に遷移中 → 何もしない
        print(f"[{now}] EC2 already in transition ({state}), skip restart")
        return False
    if state == 'stopped':
        # 停止済み → 起動するだけ
        print(f"[{now}] EC2 stopped → start ({reason})")
        ec2_client.start_instances(InstanceIds=[EC2_INSTANCE_ID])
    else:
        # running / unknown → force stop（startは次回Lambdaで処理）
        print(f"[{now}] EC2 FORCE STOP ({reason}): {EC2_INSTANCE_ID}")
        ec2_client.stop_instances(InstanceIds=[EC2_INSTANCE_ID], Force=True)
    # 再起動フラグ＆全カウンタをリセット
    set_param('restarting', 'true')
    set_count('ec2', 0)
    for n, _, _ in HTTP_CHECKS:
        set_count(f'http_{n}', 0)
    return True


# ===== メインハンドラ =====

def lambda_handler(event, context):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log = {'time': now}

    ec2_state = get_ec2_state()
    restarting = get_param('restarting', 'false') == 'true'

    # --- 遷移中処理 ---
    if ec2_state == 'stopping':
        log['status'] = 'EC2 stopping, waiting...'
        print(json.dumps(log))
        return log

    if ec2_state == 'stopped':
        print(f"[{now}] EC2 stopped → starting")
        ec2_client.start_instances(InstanceIds=[EC2_INSTANCE_ID])
        log['status'] = 'EC2 was stopped, start triggered'
        print(json.dumps(log))
        return log

    if ec2_state == 'pending':
        log['status'] = 'EC2 pending (starting up), skip checks'
        print(json.dumps(log))
        return log

    # --- EC2 running ---

    # EC2 ステータスチェック（ハードウェア/システム異常）
    ec2_ok, ec2_detail = get_ec2_status()
    ec2_failures = get_count('ec2')

    if ec2_ok:
        set_count('ec2', 0)
        if restarting:
            # 復旧確認 → HTTP OK なら restarting フラグ解除
            pass  # HTTP チェック後に判断
        log['ec2'] = {'status': 'OK', 'detail': ec2_detail}
    else:
        ec2_failures += 1
        set_count('ec2', ec2_failures)
        print(f"[{now}] EC2 status NG ({ec2_failures}/{EC2_FAIL_THRESH}): {ec2_detail}")
        log['ec2'] = {'status': 'FAILURE', 'detail': ec2_detail, 'failures': ec2_failures}
        if ec2_failures >= EC2_FAIL_THRESH:
            trigger_ec2_restart(f"EC2 status NG x{ec2_failures}")
            log['ec2']['status'] = 'RESTART_TRIGGERED'
        print(json.dumps(log))
        return log

    # --- RDS チェック ---
    rds_ok, rds_detail = check_rds()
    rds_failures = get_count('rds')
    if rds_ok:
        set_count('rds', 0)
        log['rds'] = {'status': 'OK', 'detail': rds_detail}
    else:
        rds_failures += 1
        set_count('rds', rds_failures)
        print(f"[{now}] RDS NG ({rds_failures}/3): {rds_detail}")
        log['rds'] = {'status': 'FAILURE', 'detail': rds_detail, 'failures': rds_failures}
        if rds_failures >= 3:
            rds_client.reboot_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID)
            set_count('rds', 0)
            log['rds']['status'] = 'RESTARTED'

    # --- HTTP ヘルスチェック ---
    # restarting フラグ中は initializing 扱いでスキップ（起動直後の誤検知防止）
    if restarting:
        # HTTP が全部 OK になったら restarting 解除
        all_ok = True
        http_log = {}
        for name, url, timeout in HTTP_CHECKS:
            ok, detail = check_http(name, url, timeout)
            http_log[name] = {'status': 'OK' if ok else 'RECOVERING', 'detail': detail}
            if not ok:
                all_ok = False
        log['http'] = http_log
        if all_ok:
            set_param('restarting', 'false')
            log['restarting'] = 'cleared'
            print(f"[{now}] Restart complete, all HTTP OK")
        else:
            log['restarting'] = 'in_progress'
        print(json.dumps(log))
        return log

    http_restart_triggered = False
    http_log = {}
    for name, url, timeout in HTTP_CHECKS:
        if http_restart_triggered:
            break
        ok, detail = check_http(name, url, timeout)
        cnt = get_count(f'http_{name}')
        if ok:
            set_count(f'http_{name}', 0)
            http_log[name] = {'status': 'OK', 'detail': detail, 'failures': 0}
        else:
            cnt += 1
            set_count(f'http_{name}', cnt)
            print(f"[{now}] HTTP {name} NG ({cnt}/{HTTP_FAIL_THRESH}): {detail}")
            http_log[name] = {'status': 'FAILURE', 'detail': detail, 'failures': cnt}
            if cnt >= HTTP_FAIL_THRESH:
                triggered = trigger_ec2_restart(f"HTTP {name} NG x{cnt}")
                if triggered:
                    http_log[name]['status'] = 'RESTART_TRIGGERED'
                    http_restart_triggered = True

    log['http'] = http_log
    print(json.dumps(log, ensure_ascii=False))
    return log
