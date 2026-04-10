import boto3
import json
import os
from datetime import datetime

ec2_client = boto3.client('ec2', region_name='ap-northeast-1')
rds_client = boto3.client('rds', region_name='ap-northeast-1')
ssm_client = boto3.client('ssm', region_name='ap-northeast-1')

EC2_INSTANCE_ID = 'i-014c42b58e2f8cc69'
RDS_INSTANCE_ID = 'tododb'
FAILURE_THRESHOLD = 3


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
    print(f"[{datetime.now()}] EC2 RESTART: stopping {EC2_INSTANCE_ID}")
    ec2_client.stop_instances(InstanceIds=[EC2_INSTANCE_ID])
    waiter = ec2_client.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=[EC2_INSTANCE_ID], WaiterConfig={'Delay': 15, 'MaxAttempts': 40})
    ec2_client.start_instances(InstanceIds=[EC2_INSTANCE_ID])
    print(f"[{datetime.now()}] EC2 RESTART: started {EC2_INSTANCE_ID}")


def restart_rds():
    print(f"[{datetime.now()}] RDS REBOOT: {RDS_INSTANCE_ID}")
    rds_client.reboot_db_instance(DBInstanceIdentifier=RDS_INSTANCE_ID)
    print(f"[{datetime.now()}] RDS REBOOT: triggered {RDS_INSTANCE_ID}")


def lambda_handler(event, context):
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log = {'time': now, 'ec2': {}, 'rds': {}}

    # --- EC2 check ---
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
            log['ec2'] = {'status': 'RESTARTED', 'detail': ec2_detail, 'failures': ec2_failures}
        else:
            log['ec2'] = {'status': 'FAILURE', 'detail': ec2_detail, 'failures': ec2_failures}

    # --- RDS check ---
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

    print(json.dumps(log))
    return log
