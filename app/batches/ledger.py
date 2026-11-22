import csv
import io
import json
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timezone

from app.anchors.register import survey_book
from app.capture.dialect import decode_text
from app.capture.scan import scan
from app.config import GAP_SECONDS, LATE_SECONDS, MERGE_MM, REVISION
from app.errors import TrackError
from app.manifest import canonical, digest
from app.pipeline import inspect

PARAMETERS = {'gap_s': GAP_SECONDS, 'late_s': LATE_SECONDS, 'merge_mm': MERGE_MM}


def message_id(raw):
    return digest(raw)


def _physical_lines(raw):
    """一份报文在 CSV 中占据的物理行数（含表头，含多行引号记录）。"""
    reader = csv.reader(io.StringIO(decode_text(raw), newline=''), strict=True)
    next(reader)
    last = 1
    for _ in reader:
        last = reader.line_num
    return last


def _join_messages(rows):
    """逐份去 BOM、去首尾换行后拼接，表头只保留第一份的。"""
    lines = []
    for index, raw in enumerate(rows):
        text = decode_text(raw).strip('\r\n')
        record_lines = text.splitlines()
        lines.extend(record_lines if index == 0 else record_lines[1:])
    return '\n'.join(lines).encode('utf-8')


def _line_catalog(message_rows):
    """返回每份报文在拼接报文中的全局行号区间，以及行号→报文的检索表。"""
    catalog, ends = [], []
    offset = 0
    for seq, mid, raw in message_rows:
        span = _physical_lines(raw)
        catalog.append({'seq': seq, 'message_id': mid,
                        'start_line': offset + 1, 'end_line': offset + span})
        offset += span
        ends.append(offset)
    return catalog, ends


def _message_for_line(ends, ids, line):
    return ids[bisect_left(ends, line)]


def _train_summaries(evidence):
    usage = defaultdict(lambda: {'usable_mm': 0, 'span_count': 0, 'chains': set()})
    for span in evidence['chainage']['segments']:
        item = usage[span['left']['run']]
        item['usable_mm'] += abs(span['right']['station_mm'] - span['left']['station_mm'])
        item['span_count'] += 1
        item['chains'].add(span['left']['chain'])
    projected = defaultdict(int)
    for point in evidence['chainage']['projected']:
        projected[point['run']] += 1
    # 以采信报文出现过的车次为准：区间被冲突打断时仍要显示 0，而不是把车次抹掉
    active_runs = {event['run'] for event in evidence['capture']['events']}
    trains = []
    for run in sorted(active_runs | set(usage) | set(projected)):
        item = usage.get(run)
        trains.append({'run': run,
                       'usable_mm': item['usable_mm'] if item else 0,
                       'span_count': item['span_count'] if item else 0,
                       'chains': sorted(item['chains']) if item else [],
                       'projected_count': projected.get(run, 0)})
    return trains


def _conflicts(evidence, catalog, ends):
    ids = [entry['message_id'] for entry in catalog]
    conflicts = []
    for reject in evidence['capture']['rejects']:
        if reject['reason'] != 'ID_CONFLICT':
            continue
        enriched = dict(reject)
        enriched['message_refs'] = sorted({_message_for_line(ends, ids, line)
                                           for line in reject['sources']})
        conflicts.append(enriched)
    return conflicts


def _build_reset(archive_db, channel, survey):
    rows = archive_db.execute(
        'SELECT seq, id, payload FROM messages WHERE channel=? ORDER BY seq', (channel,)).fetchall()
    catalog, ends = _line_catalog(rows)
    evidence = inspect(_join_messages([row[2] for row in rows]), survey=survey)
    message_ids = [row[1] for row in rows]
    identity = canonical({'channel': channel, 'messages': message_ids,
                          'survey_sha256': digest(canonical(survey)), 'revision': REVISION,
                          'parameters': PARAMETERS})
    reset_id = digest(identity)
    payload = {'channel': channel, 'reset_id': reset_id,
               'seq': len(rows), 'created_at': datetime.now(timezone.utc).isoformat(),
               'message_ids': message_ids, 'line_catalog': catalog,
               'conflicts': _conflicts(evidence, catalog, ends),
               'trains': _train_summaries(evidence),
               'parameters': PARAMETERS, 'stages': evidence}
    archive_db.execute('INSERT OR IGNORE INTO resets VALUES (?, ?, ?, ?, ?)',
                       (reset_id, channel, len(rows), canonical(message_ids).decode(),
                        canonical(payload).decode()))
    return payload


def ingest(archive, channel, raw):
    """采信一份报文并整批重算。解析失败直接抛错，不写入任何记录。"""
    mid = message_id(raw)
    parsed = scan(raw)  # 先校验，坏报文在开写锁前就被拒绝
    survey = survey_book()
    with archive.write_transaction() as db:
        if db.execute('SELECT 1 FROM messages WHERE channel=? AND id=?', (channel, mid)).fetchone():
            row = db.execute('SELECT payload FROM resets WHERE channel=? ORDER BY seq DESC LIMIT 1',
                             (channel,)).fetchone()
            return json.loads(row[0]), True
        seq = db.execute('SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE channel=?',
                         (channel,)).fetchone()[0]
        detail = {'seq': seq, 'message_id': mid, 'input_sha256': mid,
                  'received_at': datetime.now(timezone.utc).isoformat(),
                  'rows': parsed['input_rows'], 'accepted': parsed['accepted'],
                  'isolated': parsed['isolated'], 'duplicates': parsed['duplicates']}
        db.execute('INSERT INTO messages VALUES (?, ?, ?, ?, ?)',
                   (mid, channel, seq, raw, canonical(detail).decode()))
        payload = _build_reset(db, channel, survey)
    return payload, False


def _reset_summary(payload):
    return {'reset_id': payload['reset_id'], 'seq': payload['seq'],
            'created_at': payload['created_at'],
            'message_count': len(payload['message_ids']),
            'conflict_count': len(payload['conflicts']),
            'trains': payload['trains']}


def overview(archive, channel):
    with archive.transaction() as db:
        message_rows = db.execute(
            'SELECT detail FROM messages WHERE channel=? ORDER BY seq', (channel,)).fetchall()
        reset_rows = db.execute(
            'SELECT payload FROM resets WHERE channel=? ORDER BY seq DESC', (channel,)).fetchall()
    if not message_rows and not reset_rows:
        raise TrackError('CHANNEL_MISSING', 404)
    messages = [json.loads(row[0]) for row in message_rows]
    resets = [_reset_summary(json.loads(row[0])) for row in reset_rows]
    latest = _reset_summary(json.loads(reset_rows[0][0])) if reset_rows else None
    return {'channel': channel, 'messages': messages, 'resets': resets, 'latest': latest}


def channels(archive):
    with archive.transaction() as db:
        rows = db.execute('SELECT DISTINCT channel FROM messages ORDER BY channel').fetchall()
    return {'channels': [row[0] for row in rows]}


def get_message(archive, channel, mid):
    with archive.transaction() as db:
        row = db.execute('SELECT payload, detail FROM messages WHERE channel=? AND id=?',
                         (channel, mid)).fetchone()
    if row is None:
        raise TrackError('MESSAGE_MISSING', 404)
    return row[0], json.loads(row[1])


def get_reset(archive, reset_id):
    with archive.transaction() as db:
        row = db.execute('SELECT payload FROM resets WHERE id=?', (reset_id,)).fetchone()
    if row is None:
        raise TrackError('RESET_MISSING', 404)
    return json.loads(row[0])
