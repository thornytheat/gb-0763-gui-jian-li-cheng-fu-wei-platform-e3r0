from concurrent.futures import ThreadPoolExecutor

from app.batches.ledger import ingest, overview
from app.capture.dialect import FIELDS
from app.inputs import sample

HEADER = ','.join(FIELDS)


def rows(*lines):
    return ('﻿' + HEADER + '\n' + '\n'.join(lines)).encode('utf-8')


FIRST = rows(
    'a0,R7,anchor,2026-11-22T00:00:00Z,0,A0,3',
    'a1,R7,anchor,2026-11-22T00:01:00Z,100000,A1,3',
    'p1,R7,defect,2026-11-22T00:00:20Z,24000,擦伤,2',
)
# 后批送来同编号 a1、异值（测距 99999），两份原件必须都留档
CONFLICT = rows(
    'a1,R7,anchor,2026-11-22T00:01:00Z,99999,A1,3',
)
LATE_R8 = rows(
    'd0,R8,anchor,2026-11-22T00:01:00Z,0,D0,3',
    'd1,R8,anchor,2026-11-22T00:02:00Z,100000,D1,3',
    'dp,R8,defect,2026-11-22T00:01:30Z,30000,擦伤,2',
)


def test_first_batch_gives_usable_mileage(client):
    response = client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    assert response.status_code == 201
    payload = response.json
    train = next(item for item in payload['trains'] if item['run'] == 'R7')
    assert (train['usable_mm'], train['span_count']) == (100000, 1)
    assert payload['seq'] == 1 and payload['conflicts'] == []
    point = next(p for p in payload['stages']['chainage']['projected'] if p['event_id'] == 'p1')
    assert point['station_mm'] == 1024000


def test_conflict_batch_keeps_first_reset_and_archives_both(client):
    first = client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()}).json
    client.post('/api/batches/night-7/messages', json={'csv': CONFLICT.decode()})
    board = client.get('/api/batches/night-7').json
    assert [m['seq'] for m in board['messages']] == [1, 2]
    assert [r['seq'] for r in board['resets']] == [2, 1]
    latest = board['latest']
    # TRAP-14：冲突组每条报文各留一条隔离记录，且都指向同两份来源报文
    assert latest['conflict_count'] == 2
    latest_payload = client.get('/api/resets/' + latest['reset_id']).json
    conflict = latest_payload['conflicts'][0]
    assert conflict['sources'] == [3, 5]
    assert len(conflict['message_refs']) == 2
    assert {tuple(row['sources']) for row in latest_payload['conflicts']} == {(3, 5)}
    # 冲突基点整组隔离：错误值 99999 绝不进入采信，区间断开后该车次当前可用里程归 0
    train = next(item for item in latest['trains'] if item['run'] == 'R7')
    assert train['usable_mm'] == 0 and train['span_count'] == 0
    assert all(a['odo_mm'] != 99999 for a in latest_payload['stages']['anchors']['anchors'])
    # 交班已用的首批复位结果原样可查，仍记录 100000 mm 可用里程
    first_train = next(item for item in first['trains'] if item['run'] == 'R7')
    assert first_train['usable_mm'] == 100000
    archived = client.get('/api/resets/' + first['reset_id']).json
    assert archived['seq'] == 1 and archived['conflicts'] == []
    # 两份冲突报文原件都能从归档取回
    for mid in conflict['message_refs']:
        raw = client.get(f'/api/batches/night-7/messages/{mid}/raw')
        assert raw.status_code == 200 and raw.data
        detail = client.get(f'/api/batches/night-7/messages/{mid}').json
        assert detail['message']['message_id'] == mid


def test_each_reset_is_revisable_history(client):
    client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    client.post('/api/batches/night-7/messages', json={'csv': LATE_R8.decode()})
    board = client.get('/api/batches/night-7').json
    assert len(board['resets']) == 2
    second = client.get('/api/resets/' + board['resets'][0]['reset_id']).json
    first = client.get('/api/resets/' + board['resets'][1]['reset_id']).json
    assert {t['run'] for t in first['trains']} == {'R7'}
    assert {t['run'] for t in second['trains']} == {'R7', 'R8'}
    r8 = next(t for t in second['trains'] if t['run'] == 'R8')
    assert r8['usable_mm'] == 100000


def test_same_message_rerun_adds_no_batch(client):
    first = client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    second = client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    assert (first.status_code, second.status_code) == (201, 200)
    board = client.get('/api/batches/night-7').json
    assert len(board['messages']) == 1 and len(board['resets']) == 1


def test_failed_message_writes_nothing_and_retries(client):
    bad = client.post('/api/batches/night-7/messages', json={'csv': 'wrong'})
    assert (bad.status_code, bad.json['error']) == (422, 'BAD_HEADER')
    missing = client.get('/api/batches/night-7')
    assert missing.status_code == 404
    # 修好后重试，通道从零开始，不残留半批
    ok = client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    assert ok.status_code == 201
    board = client.get('/api/batches/night-7').json
    assert len(board['messages']) == 1


def test_channels_are_isolated(client):
    client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    other = client.post('/api/batches/night-9/messages', json={'csv': LATE_R8.decode()})
    assert [t['run'] for t in other.json['trains']] == ['R8']
    listing = client.get('/api/batches').json['channels']
    assert listing == ['night-7', 'night-9']


def test_missing_entries(client):
    assert client.get('/api/batches/nope').status_code == 404
    assert client.get('/api/resets/nope').status_code == 404
    client.post('/api/batches/night-7/messages', json={'csv': FIRST.decode()})
    response = client.get('/api/batches/night-7/messages/deadbeef')
    assert (response.status_code, response.json['error']) == (404, 'MESSAGE_MISSING')


def test_sample_ingest_and_page_markers(client):
    response = client.post('/api/batches/night-7/messages', json={'sample': 'dirty'})
    assert response.status_code == 201
    page = client.get('/work/runs').get_data(as_text=True)
    for marker in ('车次复位', '同编号异值冲突报文', '每次复位结果', '夜巡报文分批回传', '归档报文'):
        assert marker in page


def test_concurrent_identical_messages_single_commit(archive_app):
    archive = archive_app.track_archive
    with ThreadPoolExecutor(6) as pool:
        results = list(pool.map(lambda _: ingest(archive, 'night-7', sample('dirty')), range(8)))
    assert sum(not reused for _, reused in results) == 1
    board = overview(archive, 'night-7')
    assert (len(board['messages']), len(board['resets'])) == (1, 1)
