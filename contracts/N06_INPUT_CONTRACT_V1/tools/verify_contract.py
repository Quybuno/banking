"""Verify this contract package, not the future pipeline. Python 3.10+, stdlib only.

Supports only the JSON Schema keywords used in this package; fails on unknown ones.
Run from any directory: python <package>/tools/verify_contract.py
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
import uuid
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / 'fixtures' / 'acceptance'
CHECKS: list[str] = []
SUPPORTED = {'$schema', '$id', '$ref', 'type', 'additionalProperties', 'properties',
             'required', 'const', 'enum', 'format', 'pattern', 'minimum', 'maximum',
             'minLength', 'minItems', 'items', 'oneOf', 'allOf', 'if', 'then'}


def load(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def rows(name: str):
    return [json.loads(line) for line in (FIX / name).read_text(encoding='utf-8').splitlines()]


SCHEMAS = {p.name: load(p) for p in (ROOT / 'schemas').glob('*.schema.json')}


def require(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def passed(name: str):
    CHECKS.append(name)
    print('PASS:', name)


def validate(value, schema, path='$') -> list[str]:
    """Small fail-closed validator for the package's schema subset."""
    unknown = set(schema) - SUPPORTED
    if unknown:
        raise RuntimeError(f'Unsupported schema keywords: {sorted(unknown)}')
    if '$ref' in schema:
        return validate(value, SCHEMAS[schema['$ref']], path)
    errors = []
    typ = schema.get('type')
    types = {'object': lambda v: isinstance(v, dict),
             'array': lambda v: isinstance(v, list),
             'string': lambda v: isinstance(v, str),
             'integer': lambda v: isinstance(v, int) and not isinstance(v, bool)}
    if typ and not types[typ](value):
        return [f'{path}: type {typ}']
    if 'const' in schema and value != schema['const']:
        errors.append(f'{path}: const')
    if 'enum' in schema and value not in schema['enum']:
        errors.append(f'{path}: enum')
    if isinstance(value, dict):
        props = schema.get('properties', {})
        for key in schema.get('required', []):
            if key not in value:
                errors.append(f'{path}.{key}: required')
        if schema.get('additionalProperties') is False:
            errors.extend(f'{path}.{k}: additional property' for k in value.keys() - props.keys())
        for key in value.keys() & props.keys():
            errors.extend(validate(value[key], props[key], path+'.'+key))
    if isinstance(value, list):
        if len(value) < schema.get('minItems', 0):
            errors.append(f'{path}: minItems')
        if 'items' in schema:
            for i, item in enumerate(value):
                errors.extend(validate(item, schema['items'], f'{path}[{i}]'))
    if isinstance(value, str):
        if len(value) < schema.get('minLength', 0):
            errors.append(f'{path}: minLength')
        if 'pattern' in schema and not re.search(schema['pattern'], value):
            errors.append(f'{path}: pattern')
        try:
            fmt = schema.get('format')
            if fmt == 'uuid':
                uuid.UUID(value)
            elif fmt == 'date':
                require(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}', value)), 'date format')
                date.fromisoformat(value)
            elif fmt == 'date-time':
                require(bool(re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', value)), 'UTC timestamp format')
                datetime.fromisoformat(value.replace('Z', '+00:00'))
        except (ValueError, AssertionError):
            errors.append(f'{path}: format {schema.get("format")}')
    if isinstance(value, int) and not isinstance(value, bool):
        if 'minimum' in schema and value < schema['minimum']:
            errors.append(f'{path}: minimum')
        if 'maximum' in schema and value > schema['maximum']:
            errors.append(f'{path}: maximum')
    if 'oneOf' in schema and sum(not validate(value, part, path) for part in schema['oneOf']) != 1:
        errors.append(f'{path}: oneOf')
    for part in schema.get('allOf', []):
        errors.extend(validate(value, part, path))
    if 'if' in schema and not validate(value, schema['if'], path):
        errors.extend(validate(value, schema.get('then', {}), path))
    return errors


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def business_date(value):
    # Package fixtures are in 2026; Ho Chi Minh is UTC+07 with no DST in 2026.
    return timestamp(value).astimezone(timezone(timedelta(hours=7))).date().isoformat()


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def draw(version, seed, label, ordinal, low, high):
    """Reference implementation of the deterministic draw primitive (contract section 5.2)."""
    width = high - low + 1
    limit = (1 << 256) - ((1 << 256) % width)
    counter = 0
    while True:
        digest = hashlib.sha256(canonical([version, seed, label, ordinal, counter]).encode('utf-8')).digest()
        number = int.from_bytes(digest, 'big')
        if number < limit:
            return low + number % width
        counter += 1


def det_uuid(namespace, version, dataset_id, seed, entity, ordinal, discriminator):
    """Reference implementation of deterministic UUIDv5 identities (contract section 5.2)."""
    return str(uuid.uuid5(uuid.UUID(namespace), canonical([version, dataset_id, seed, entity, ordinal, discriminator])))


def record_error(record, accounts):
    """Reference classification for negative record fixtures only."""
    props = SCHEMAS['transaction.schema.json']['properties']
    if record.get('schema_version') != '1.0.0':
        return 'UNSUPPORTED_SCHEMA_VERSION'
    if set(SCHEMAS['transaction.schema.json']['required']) - record.keys():
        return 'MISSING_REQUIRED_FIELD'
    errors = validate(record, SCHEMAS['transaction.schema.json'])
    if any(': type ' in e for e in errors):
        return 'SCHEMA_TYPE'
    if record['currency'] not in props['currency']['enum']:
        return 'UNSUPPORTED_CURRENCY'
    if not 1 <= record['amount_minor'] <= 10**12:
        return 'AMOUNT_OUT_OF_RANGE'
    if errors:
        return 'SCHEMA_VALUE'
    if business_date(record['event_time']) != record['business_date']:
        return 'BUSINESS_DATE_MISMATCH'
    if record['account_from'] == record['account_to']:
        return 'SAME_ACCOUNT_TRANSFER'
    if any(record[k] not in accounts for k in ['account_from', 'account_to']):
        return 'UNKNOWN_ACCOUNT'
    if any(accounts[record[k]]['currency'] != record['currency'] for k in ['account_from','account_to']):
        return 'ACCOUNT_CURRENCY_MISMATCH'
    return None


def main():
    manifest = load(FIX / 'source_manifest.json')
    require(not validate(manifest, SCHEMAS['source_manifest.schema.json']), 'Manifest schema')
    for item in manifest['files']:
        p = (ROOT / item['path']).resolve()
        require(p.is_relative_to(ROOT), 'Manifest path escapes package')
        data = p.read_bytes()
        require(not data.startswith(b'\xef\xbb\xbf') and b'\r' not in data, f'Encoding/LF: {p.name}')
        require(hashlib.sha256(data).hexdigest() == item['sha256'], f'Checksum {p.name}')
        require(len(data.decode('utf-8').splitlines()) == item['physical_rows'], f'Count {p.name}')
    passed('Manifest SHA-256, physical rows, UTF-8/LF and safe paths')

    accounts, txs, entries, events, statements, deliveries = [rows(f'{n}.jsonl') for n in ['accounts','transactions','ledger_entries','events','statements','statement_deliveries']]
    for name, records in [('account',accounts),('transaction',txs),('ledger_entry',entries),('event_envelope',events),('statement',statements),('statement_delivery',deliveries)]:
        for i, record in enumerate(records):
            errors = validate(record, SCHEMAS[name+'.schema.json'])
            require(not errors, f'{name} row {i}: {errors}')
    passed('All positive fixture records satisfy package schema subset with format checks')

    for profile in ['demo-clean','demo-faults','bench-skew']:
        config=load(ROOT / 'config' / f'generator.{profile}.json')
        require(not validate(config, SCHEMAS['generator_config.schema.json']), f'Config schema {profile}')
        require(config['from_date'] <= config['to_date'], 'Date range')
        require(config['amount_min_minor'] <= config['amount_max_minor'], 'Amount range')
        require(sum(config['channel_weights_basis_points'].values()) == 10000, 'Channel weights')
        require(sum(v for k,v in config['faults'].items() if k.endswith('_basis_points')) <= 10000, 'Fault sum')
        require(config['late_within_delay_after_day_end_seconds'] + config['statement_file_window_seconds'] < config['allowed_lateness_seconds'] <= config['late_after_delay_after_day_end_seconds'], 'Late delays and statement file window')
        if config['account_selection'] == 'HOT_ACCOUNT_V1':
            require(1 <= config['hot_account_count'] < config['account_count'] and config['hot_account_basis_points'] > 0, 'Hot account settings')
        else:
            require(config['hot_account_count'] == 0 and config['hot_account_basis_points'] == 0, 'Hot account settings must be zero')
    passed('Generator config schemas and cross-field constraints')

    account_map={a['account_id']:a for a in accounts}
    for collection, key in [(accounts,'account_id'),(txs,'transaction_id'),(entries,'entry_id'),(statements,'statement_record_id')]:
        require(len({r[key] for r in collection}) == len(collection), f'Unique {key}')
    require(len({(t['channel'],t['external_reference']) for t in txs})==len(txs), 'Unique transaction references')
    for t in txs:
        require(record_error(t,account_map) is None, f'Invalid business transaction {t["external_reference"]}')
    for r in entries+statements:
        require(business_date(r['event_time'])==r['business_date'], 'Business date')
    for s in statements:
        require(timestamp(s['available_at'])>=timestamp(s['event_time']), 'Statement chronology')
    passed('Business keys, account references, currencies, dates and chronology')

    event_map={}
    for e in events:
        require(e['dataset_id']==e['payload']['dataset_id'], 'Envelope dataset')
        require(e['event_time']==e['payload']['event_time'], 'Envelope time')
        require(timestamp(e['emitted_at'])>=timestamp(e['event_time']), 'Envelope chronology')
        require(e['source_batch_id']==manifest['source_batch_id'], 'Envelope batch')
        if e['event_id'] in event_map:
            require(canonical(e)==canonical(event_map[e['event_id']]), 'Unexpected event conflict')
        event_map[e['event_id']]=e
    business_records={}
    for e in event_map.values():
        p=e['payload']; key=('T',p['transaction_id']) if e['event_type']=='TRANSACTION_CREATED' else ('L',p['entry_id'])
        if key in business_records:
            require(p==business_records[key], 'Unexpected business conflict')
        business_records[key]=p
    require({k[1]:v for k,v in business_records.items() if k[0]=='T'}=={t['transaction_id']:t for t in txs}, 'Events/reference transaction equality')
    require({k[1]:v for k,v in business_records.items() if k[0]=='L'}=={e['entry_id']:e for e in entries}, 'Events/reference ledger equality')
    passed('Envelope consistency; replay dedup yields exact reference business records')

    tx_map={t['transaction_id']:t for t in txs}; grouped=defaultdict(list)
    for e in entries:
        require(e['transaction_id'] in tx_map,'Ledger unknown transaction')
        grouped[e['transaction_id']].append(e)
    for t in txs:
        group=grouped[t['transaction_id']]
        if t['status']=='FAILED':
            require(not group,'Failed transaction has ledger')
            continue
        require(len(group)==2 and {e['side'] for e in group}=={'DEBIT','CREDIT'},'Two ledger sides')
        for e in group:
            require(e['ledger_account_id']==t['account_from' if e['side']=='DEBIT' else 'account_to'],'Ledger account')
            require(all(e[k]==t[k] for k in ['amount_minor','currency','event_time','business_date']),'Ledger/transaction content')
    passed('Ledger completeness, balanced sides and transaction linkage')

    expected=load(FIX / 'expected_results.json')
    actual_counts={'accounts':len(accounts),'transactions':len(txs),'success_transactions':sum(t['status']=='SUCCESS' for t in txs),'failed_transactions':sum(t['status']=='FAILED' for t in txs),'ledger_entries':len(entries),'physical_events':len(events),'unique_event_ids':len(event_map),'unique_business_records':len(business_records),'statement_business_records_all_released':len(statements),'match_keys':len({(r['channel'],r['external_reference']) for r in txs+statements})}
    require(actual_counts==expected['counts'], f'Expected counts: {actual_counts}')
    totals={'transactions_VND_minor':sum(t['amount_minor'] for t in txs),'success_transactions_VND_minor':sum(t['amount_minor'] for t in txs if t['status']=='SUCCESS'),'ledger_debit_VND_minor':sum(e['amount_minor'] for e in entries if e['side']=='DEBIT'),'ledger_credit_VND_minor':sum(e['amount_minor'] for e in entries if e['side']=='CREDIT'),'statement_VND_minor_all_released':sum(s['amount_minor'] for s in statements if s['currency']=='VND'),'statement_USD_minor_all_released':sum(s['amount_minor'] for s in statements if s['currency']=='USD')}
    require(totals==expected['totals'],f'Expected totals: {totals}')
    passed('Hand-authored expected counts and per-currency control totals')

    control=load(FIX / 'control_report.json')
    require(not validate(control, SCHEMAS['control_report.schema.json']), 'Control report schema')
    require(control['dataset_id']==manifest['dataset_id'], 'Control report dataset')
    order=lambda r:(r['entity'],r['business_date'],r['currency'],r['status_or_side'])
    require(all(r['record_count']>=1 for r in control['totals']), 'Control report has no zero rows')
    require([order(r) for r in control['totals']]==sorted({order(r) for r in control['totals']}), 'Control report rows unique and sorted')
    require(all((r['entity']=='transaction')==(r['status_or_side'] in ('SUCCESS','FAILED')) for r in control['totals']), 'Control report status_or_side matches entity')
    derived=defaultdict(lambda:[0,0])
    for t in txs:
        d=derived[('transaction',t['business_date'],t['currency'],t['status'])]; d[0]+=1; d[1]+=t['amount_minor']
    for e in entries:
        d=derived[('ledger_entry',e['business_date'],e['currency'],e['side'])]; d[0]+=1; d[1]+=e['amount_minor']
    require({order(r):[r['record_count'],r['amount_minor_total']] for r in control['totals']}=={k:v for k,v in derived.items()}, 'Hand-authored control report equals reference business records')
    passed('Hand-authored control report: schema, row rules and per-group totals')

    balances={a['account_id']:a['opening_balance_minor'] for a in accounts}
    snapshots=[]
    for day in sorted({t['business_date'] for t in txs}):
        for account in accounts:
            aid=account['account_id']; opening=balances[aid]
            debit=sum(e['amount_minor'] for e in entries if e['business_date']==day and e['ledger_account_id']==aid and e['side']=='DEBIT')
            credit=sum(e['amount_minor'] for e in entries if e['business_date']==day and e['ledger_account_id']==aid and e['side']=='CREDIT')
            balances[aid]=opening+credit-debit
            require(balances[aid]>=0,'Overdraft')
            snapshots.append({'business_date':day,'account_id':aid,'currency':account['currency'],'opening_minor':opening,'debit_minor':debit,'credit_minor':credit,'closing_minor':balances[aid]})
    require(snapshots==expected['balances'],'Hand-authored balances')
    # Verify no intraday overdraft too.
    current={a['account_id']:a['opening_balance_minor'] for a in accounts}
    for t in sorted(txs,key=lambda t:(t['event_time'],t['transaction_id'])):
        if t['status']=='SUCCESS':
            current[t['account_from']]-=t['amount_minor'];current[t['account_to']]+=t['amount_minor']
            require(current[t['account_from']]>=0,'Intraday overdraft')
    passed('Two-day opening/movement/closing arithmetic and no overdraft')

    delivered=[]
    require(len(deliveries)==len(statements),'Delivery coverage')
    for delivery in deliveries:
        file_rows=[json.loads(line) for line in (ROOT/delivery['file_path']).read_text(encoding='utf-8').splitlines()]
        found=[s for s in file_rows if s['statement_record_id']==delivery['statement_record_id']]
        require(len(found)==1 and found[0]['available_at']==delivery['file_available_at'],'Release timing')
        delivered.extend(found)
    require({s['statement_record_id']:s for s in delivered}=={s['statement_record_id']:s for s in statements},'Batch/reference statement equality')
    passed('Actual statement batches and delivery schedule equal reference without early release')

    for snapshot in expected['snapshots']:
        asof=timestamp(snapshot['as_of_time'])
        available=[s for s in delivered if timestamp(s['available_at'])<=asof]
        internal={(t['channel'],t['external_reference']):t for t in txs if timestamp(t['event_time'])<=asof}
        external=defaultdict(list)
        for s in available:external[(s['channel'],s['external_reference'])].append(s)
        results={}
        for key in internal.keys()|external.keys():
            t=internal.get(key); group=external.get(key,[])
            if len(group)>1:status='DUPLICATE_OR_CONFLICT'
            elif t and group:
                s=group[0]
                status='CURRENCY_MISMATCH' if t['currency']!=s['currency'] else 'AMOUNT_MISMATCH' if t['amount_minor']!=s['amount_minor'] else 'STATUS_MISMATCH' if t['status']!=s['status'] else 'MATCHED'
            else:
                row=t or group[0]
                next_day=date.fromisoformat(row['business_date'])+timedelta(days=1)
                cutoff=datetime.combine(next_day,datetime.min.time(),timezone(timedelta(hours=7)))+timedelta(seconds=86400)
                status='PENDING' if asof<cutoff else 'MISSING_EXTERNAL' if t else 'MISSING_INTERNAL'
            results[key[1]]=status
        require(results==snapshot['expected_status_by_reference'],f'Snapshot per key {snapshot["name"]}')
        require(dict(Counter(results.values()))==snapshot['expected_counts'],f'Snapshot histogram {snapshot["name"]}')
    passed('All three reconciliation snapshots match every key and expected histograms')

    cutoff_cases=load(FIX/'cutoff_cases.json')
    cutoff=timestamp(cutoff_cases['cutoff_utc'])
    for case in cutoff_cases['cases']:
        status='PENDING' if timestamp(case['as_of_time'])<cutoff else 'MISSING_EXTERNAL'
        require(status==case['expected_status'],'Cutoff inclusive boundary')
    passed('Before, exactly at and after cutoff use an unambiguous inclusive deadline')

    negative=ROOT/'fixtures'/'negative'
    cases=[json.loads(line) for line in (negative/'record_cases.jsonl').read_text(encoding='utf-8').splitlines()]
    for case in cases:
        require(record_error(case['input'],account_map)==case['expected_error'],f'Negative {case["case_id"]}')
    for file,key in [('event_id_conflict.jsonl','event_id'),('business_key_conflict.jsonl','event_id')]:
        x,y=[json.loads(line) for line in (negative/file).read_text(encoding='utf-8').splitlines()]
        require(x['payload']['transaction_id']==y['payload']['transaction_id'] and x['payload']!=y['payload'],'Negative business conflict')
        require((x[key]==y[key])==(file=='event_id_conflict.jsonl'),'Negative event identity')
    unbalanced=[json.loads(line) for line in (negative/'unbalanced_ledger.jsonl').read_text(encoding='utf-8').splitlines()]
    require(sum(x['amount_minor']*(1 if x['side']=='DEBIT' else -1) for x in unbalanced)!=0,'Negative imbalance')
    require(len((negative/'missing_ledger_side.jsonl').read_text(encoding='utf-8').splitlines())==1,'Negative incomplete ledger')
    for line in (negative/'malformed_payloads.txt').read_text(encoding='utf-8').splitlines():
        try:json.loads(line)
        except json.JSONDecodeError:continue
        raise AssertionError('Malformed fixture unexpectedly valid')
    passed('Negative records, ID conflicts, incomplete/unbalanced ledger and malformed JSON')

    golden=load(ROOT/'fixtures'/'golden'/'deterministic_vectors.json')
    gv,gs,gd,gn=golden['contract_version'],golden['seed'],golden['dataset_id'],golden['seed_namespace']
    for case in golden['draw']:
        require(draw(gv,gs,case['label'],case['ordinal'],case['low'],case['high'])==case['value'],f'Golden draw {case["label"]}/{case["ordinal"]}')
    for case in golden['uuid']:
        require(det_uuid(gn,gv,gd,gs,case['entity'],case['ordinal'],case['discriminator'])==case['value'],f'Golden uuid {case["entity"]}/{case["ordinal"]}')
    for case in golden['external_reference']:
        ref='REF-'+det_uuid(gn,gv,gd,gs,'transaction',case['transaction_ordinal'],'original').replace('-','').upper()
        require(ref==case['value'],'Golden external reference')
    passed('Golden vectors for draw(), UUIDv5 identities and external_reference')

    report={'status':'PASS','scope':'Contract package only; no generator/Kafka/Spark/PostgreSQL execution verified','checks':CHECKS,'counts':actual_counts,'totals':totals,'snapshot_histograms':{s['name']:s['expected_counts'] for s in expected['snapshots']}}
    report_path=ROOT/'VERIFICATION_REPORT.json'
    report_path.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    print(f'\n{len(CHECKS)} check groups passed. Report: {report_path}')


if __name__=='__main__':
    try:
        main()
    except Exception as exc:
        print(f'FAIL: {exc}',file=sys.stderr)
        sys.exit(1)
