from flask import Blueprint, current_app, render_template, request

from app.archive.save import recent
from app.batches.ledger import overview
from app.config import REVISION
from app.errors import TrackError
from app.inputs import sample
from app.pipeline import inspect

pages = Blueprint('track_web', __name__)
SECTIONS = {'coverage': '轨段覆盖对账', 'capture': '轨检报文采信', 'anchors': '基点匹配',
            'chainage': '区间坐标复位', 'defects': '缺陷投影合并', 'runs': '批次留档复跑',
            'issue': '巡检证据签发'}


def _batch(channel):
    try:
        return overview(current_app.track_archive, channel)
    except TrackError as exc:
        if exc.code != 'CHANNEL_MISSING':
            raise
        return None


@pages.get('/')
@pages.get('/work/<section>')
def screen(section='coverage'):
    if section not in SECTIONS:
        raise TrackError('NO_SAMPLE', 404)
    label = request.args.get('sample', 'dirty')
    results = inspect(sample(label), section if section not in ('runs', 'issue') else 'coverage')
    ledger = recent(current_app.track_archive)['runs'] if section in ('runs', 'issue') else []
    channel = request.args.get('channel', 'night-7')
    batch = _batch(channel) if section == 'runs' else None
    return render_template('base.html', section=section, nav=SECTIONS, label=label, stages=results,
                           ledger=ledger, rev=REVISION, run_id=request.args.get('run', ''),
                           channel=channel, batch=batch)


@pages.get('/work/resets/<reset_id>')
def reset_screen(reset_id):
    from app.batches.ledger import get_reset
    payload = get_reset(current_app.track_archive, reset_id)
    return render_template('reset.html', nav=SECTIONS, rev=REVISION, payload=payload)
