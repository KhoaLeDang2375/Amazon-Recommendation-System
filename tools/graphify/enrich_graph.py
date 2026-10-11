"""Add deterministic Compose and Markdown navigation to Graphify's AST graph.

No data files, environment secrets, model contents, or LLM calls are read.
"""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
path = ROOT / 'graphify-out/graph.json'
graph = json.loads(path.read_text(encoding='utf-8'))
nodes = {node['id']: node for node in graph['nodes']
         if node.get('_origin') != 'config_navigation'}
edges = [edge for edge in graph.get('edges', graph.get('links', []))
         if edge.get('_origin') != 'config_navigation']

def add_node(identifier, label, source, line=1, kind='file'):
    nodes[identifier] = dict(id=identifier, label=label, file_type='code' if source.endswith('.py') else 'document',
                             source_file=source, source_location=f'L{line}',
                             metadata={'kind': kind}, _origin='config_navigation')
    return identifier

def link(source, target, relation, filename):
    edges.append(dict(source=source, target=target, relation=relation,
                      confidence='EXTRACTED', source_file=filename,
                      source_location='L1', weight=1.0, _origin='config_navigation'))

compose = add_node('file:compose.yaml', 'Compose services, ports, mounts and resource limits', 'compose.yaml')
lines = (ROOT / 'compose.yaml').read_text(encoding='utf-8').splitlines()
service = None
in_services = False
for index, line in enumerate(lines, 1):
    if line == 'services:':
        in_services = True
    elif line and not line.startswith(' ') and line != 'services:':
        in_services = False
    if not in_services:
        continue
    match = re.fullmatch(r'  ([a-z][\w-]*):', line)
    if match:
        service = add_node('service:' + match[1], match[1] + ' Compose service', 'compose.yaml', index, 'service')
        link(compose, service, 'defines', 'compose.yaml')
    elif service:
        dependency = re.fullmatch(r'      ([a-z][\w-]*):', line)
        if dependency:
            link(service, 'service:' + dependency[1], 'depends_on', 'compose.yaml')
        mount = re.match(r'      - (\./[^:]+):', line)
        if mount:
            source = mount[1][2:]
            target = add_node('path:' + source, source + ' mounted path', 'compose.yaml', index, 'mount')
            link(service, target, 'mounts', 'compose.yaml')

files = [ROOT / 'README.md', ROOT / 'AGENTS.md']
files += sorted((ROOT / 'docs').glob('*.md'))
files += [ROOT / 'ingestion/README.md', ROOT / 'tests/README.md']
for file in files:
    source = file.relative_to(ROOT).as_posix()
    identifier = add_node('file:' + source, source, source)
    link(compose, identifier, 'documented_by', source)
    for index, line in enumerate(file.read_text(encoding='utf-8').splitlines(), 1):
        if re.match(r'^#{1,3} ', line):
            heading = add_node(f'{identifier}:L{index}', line.lstrip('# '), source, index, 'heading')
            link(identifier, heading, 'contains', source)

for service_name, directory in [('flume', 'infrastructure/flume'), ('namenode', 'infrastructure/hadoop'),
                                 ('datanode', 'infrastructure/hadoop'), ('spark-notebook', 'infrastructure/spark'),
                                 ('graphify', 'infrastructure/graphify')]:
    for file in sorted((ROOT / directory).rglob('*')):
        if file.is_file():
            source = file.relative_to(ROOT).as_posix()
            existing = next((node['id'] for node in nodes.values()
                             if node.get('source_file') == source
                             and node.get('metadata', {}).get('kind') == 'file'), None)
            identifier = existing or add_node('file:' + source, source, source)
            link('service:' + service_name, identifier, 'configured_by', source)

graph['nodes'] = list(nodes.values())
graph['edges'] = edges
graph.pop('links', None)
path.write_text(json.dumps(graph, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(f"Navigation graph: {len(nodes)} nodes, {len(edges)} edges")
