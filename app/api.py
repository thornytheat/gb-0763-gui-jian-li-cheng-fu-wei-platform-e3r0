from flask import Blueprint, current_app, jsonify, Response

from app.archive.save import recent, replay, retrieve
from app.batches.ledger import channels, get_message, get_reset, ingest, overview
from app.inputs import source
from app.pipeline import inspect
from app.seals.issue import issue, verified

api = Blueprint('track_api', __name__)


def stage(name):
    return inspect(source(), name)[name]


@api.route('/api/capture', methods=['GET', 'POST'])
def capture():
    return stage('capture')


@api.route('/api/anchors', methods=['GET', 'POST'])
def anchors():
    return stage('anchors')


@api.route('/api/chainage', methods=['GET', 'POST'])
def chainage():
    return stage('chainage')


@api.route('/api/defects', methods=['GET', 'POST'])
def defects():
    return stage('defects')


@api.route('/api/coverage', methods=['GET', 'POST'])
def coverage():
    return stage('coverage')


@api.get('/api/runs')
def history():
    return recent(current_app.track_archive)


@api.post('/api/runs')
def run():
    data = replay(current_app.track_archive, source())
    return data, 200 if data['reused'] else 201


@api.get('/api/runs/<run_id>')
def get_run(run_id):
    return retrieve(current_app.track_archive, run_id)


@api.post('/api/runs/<run_id>/issue')
def sign(run_id):
    return issue(current_app.track_archive, run_id)


@api.get('/api/runs/<run_id>/export')
def export(run_id):
    response = jsonify(verified(current_app.track_archive, run_id))
    response.headers['Content-Disposition'] = f'attachment; filename="track-{run_id}.json"'
    return response


@api.get('/api/batches')
def batch_channels():
    return channels(current_app.track_archive)


@api.post('/api/batches/<channel>/messages')
def post_message(channel):
    data, reused = ingest(current_app.track_archive, channel, source())
    return jsonify(data), 200 if reused else 201


@api.get('/api/batches/<channel>')
def batch_overview(channel):
    return overview(current_app.track_archive, channel)


@api.get('/api/batches/<channel>/messages/<message_id>')
def read_message(channel, message_id):
    raw, detail = get_message(current_app.track_archive, channel, message_id)
    return jsonify({'message': detail, 'raw': raw.decode('utf-8-sig')})


@api.get('/api/batches/<channel>/messages/<message_id>/raw')
def raw_message(channel, message_id):
    raw, _ = get_message(current_app.track_archive, channel, message_id)
    response = Response(raw, mimetype='text/csv')
    response.headers['Content-Disposition'] = f'attachment; filename="message-{message_id[:12]}.csv"'
    return response


@api.get('/api/resets/<reset_id>')
def read_reset(reset_id):
    return get_reset(current_app.track_archive, reset_id)
