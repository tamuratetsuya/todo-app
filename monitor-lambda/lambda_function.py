import boto3
import json
import os
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

EC2_INSTANCE_ID = 'i-014c42b58e2f8cc69'
RDS_INSTANCE_ID = 'tododb'
FAILURE_THRESHOLD = 3       # EC2/RDS: 3回連続NGでリブート
HTTP_FAILURE_THRESHOLD = 5  # HTTP: 5回連続NGでリブート（起動に時間がかかる場合の誤検知防止）

# HTTPヘルスチェック対象（応答があればOK、ステータスコード問わず）
# stock.golfspace.jpのDNSが引けない場合はIPで直接チェック
HTTP_CHECKS = [
    ("nginx",   "http://57.183.7.38/stock.html",    8),
    ("stock",   "http://57.183.7.38/api/stock/",   10),
    ("calendar","https://calendar.golfspace.jp/api/calendar/events", 10),
]


def check_ec2():
    try:
        resp = ec2_client.describe_instance_status(
            InstanceIds=[EC2_INSTANCE_ID],
            IncludeAllInstances=True
        )
        statuses = resp['InstanceStatuses']
        if not statuses:
            return False, 'no status'
        s = statuses[0]
        state = s['InstanceState']['Name']
        inst_status = s['InstanceStatus']['Status']
        sys_status = s['SystemStatus']['Status']
        ok = (state == 'running' and inst_status == 'ok' and sys_status == 'ok')
        return ok, f"state={state} inst={inst_status} sys={sys_status}"
    except Exception as e:
        return False, str(e)


def check_rds():
    try:
        resp = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_ID)
        db = resp['DBInstances'][0]
        status = db['DBInstanceStatus']
        ok = (status == 'available')
        return ok, f"status={status}"
    except Exception as e:
        return False, str(e)


def check_http(name, url, timeout):
    """URLにリクエストしてタイムアウト内に応答があればOK（ステータスコード問わず）"""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "golfspace-monitor/1.0"})
        with urllib.request.urlopen(req, timeout=timeout, context=_ssl_ctx) as resp:
            return True, f"http={resp.status}"
    except urllib.error.HTTPError as e:
        # HTTPエラー（4xx/5xx）でもサーバーが応答しているのでOK
        return True, f"http={e.code}"
    except Exception as e:
        return False, str(e)


def get_failure_count(key):
    try:
        resp = ssm_client.get_parameter(Name=f'/golfspace-monitor/{key}/failures')
        return int(resp['Parameter']['Value'])
    except:
        return 0


def set_failure_count(key, count):
    ssm_client.put_parameter(
        Name=f'/golfspace-monitor/{key}/failures',
        Value=str(count),
        Type='String',
        Overwrite=True
    )


def restart_ec2():
    print(f"[{datetime.now()}] EC2 REBOOT: {EC2_INSTANCE_ID}")
    ec2_client.reboot_instances(InstanceIds=[EC2_INSTANCE_ID])
    print(f"[{datetime.now()}] EC2 REBOOT: triggered")


def restart_rds():
    print(f"[{datetime.now()}] RDS REBOOT: {RDS_INSTANCE_ID}")
    rds_client.reboot_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID)
    print(f"[{datetime.now()}] RDS REBOOT: triggered {RDS_INSTANCE_ID}")


def lambda_handler(event, context):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log = {'time': now, 'ec2': {}, 'rds': {}, 'http': {}}

    # --- EC2インスタンス状態チェック ---
    ec2_ok, ec2_detail = check_ec2()
    ec2_failures = get_failure_count('ec2')

    if ec2_ok:
        set_failure_count('ec2', 0)
        log['ec2'] = {'status': 'OK', 'detail': ec2_detail, 'failures': 0}
    else:
        ec2_failures += 1
        set_failure_count('ec2', ec2_failures)
        print(f"[{now}] EC2 NG ({ec2_failures}/{FAILURE_THRESHOLD}): {ec2_detail}")

        if ec2_failures >= FAILURE_THRESHOLD:
            restart_ec2()
            set_failure_count('ec2', 0)
            log['ec2'] = {'status': 'REBOOTED', 'detail': ec2_detail, 'failures': ec2_failures}
        else:
            log['ec2'] = {'status': 'FAILURE', 'detail': ec2_detail, 'failures': ec2_failures}

    # --- RDSチェック ---
    rds_ok, rds_detail = check_rds()
    rds_failures = get_failure_count('rds')

    if rds_ok:
        set_failure_count('rds', 0)
        log['rds'] = {'status': 'OK', 'detail': rds_detail, 'failures': 0}
    else:
        rds_failures += 1
        set_failure_count('rds', rds_failures)
        print(f"[{now}] RDS NG ({rds_failures}/{FAILURE_THRESHOLD}): {rds_detail}")

        if rds_failures >= FAILURE_THRESHOLD:
            restart_rds()
            set_failure_count('rds', 0)
            log['rds'] = {'status': 'RESTARTED', 'detail': rds_detail, 'failures': rds_failures}
        else:
            log['rds'] = {'status': 'FAILURE', 'detail': rds_detail, 'failures': rds_failures}

    # --- HTTPヘルスチェック（アプリ応答確認）---
    # EC2がOKでもアプリがハングしている場合を検知してリブート
    # EC2インスタンス自体がNGの場合はHTTPチェックをスキップ（リブート中などで誤検知を防ぐ）
    if not ec2_ok:
        log['http'] = {'skipped': 'EC2 not OK'}
        print(json.dumps(log, ensure_ascii=False))
        return log

    for name, url, timeout in HTTP_CHECKS:
        ok, detail = check_http(name, url, timeout)
        http_failures = get_failure_count(f'http_{name}')

        if ok:
            set_failure_count(f'http_{name}', 0)
            log['http'][name] = {'status': 'OK', 'detail': detail, 'failures': 0}
        else:
            http_failures += 1
            set_failure_count(f'http_{name}', http_failures)
            print(f"[{now}] HTTP {name} NG ({http_failures}/{FAILURE_THRESHOLD}): {detail}")
            any_http_ng = True

            if http_failures >= HTTP_FAILURE_THRESHOLD:
                print(f"[{now}] HTTP {name} {HTTP_FAILURE_THRESHOLD}連続失敗 → EC2リブート")
                restart_ec2()
                # 全HTTPカウンタをリセット（リブート後に再カウント）
                for n, _, _ in HTTP_CHECKS:
                    set_failure_count(f'http_{n}', 0)
                log['http'][name] = {'status': 'REBOOTED', 'detail': detail, 'failures': http_failures}
                break  # リブート済みなので残りのチェック不要
            else:
                log['http'][name] = {'status': 'FAILURE', 'detail': detail, 'failures': http_failures}

    print(json.dumps(log, ensure_ascii=False))
    return log
