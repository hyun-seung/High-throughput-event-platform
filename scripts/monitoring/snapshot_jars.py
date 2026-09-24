"""Bind immutable copies: rebuilding target/*.jar must not change a running JVM's files."""
import hashlib
from pathlib import Path
import shutil
ROOT=Path(__file__).resolve().parents[2]
DIRECTORY=ROOT/'.monitoring'
DIRECTORY.mkdir(exist_ok=True)
lines=[]
for name,module in [('API','event-api'),('INGRESS','delivery-ingress-worker'),('DISPATCH','dispatch-worker'),('SIMULATOR','external-api-simulator')]:
    source=ROOT/module/'target'/f'{module}-1.0-SNAPSHOT.jar'
    digest=hashlib.sha256(source.read_bytes()).hexdigest()
    target=DIRECTORY/'jars'/digest/(module+'.jar')
    target.parent.mkdir(parents=True,exist_ok=True)
    if not target.exists():shutil.copyfile(source,target)
    if hashlib.sha256(target.read_bytes()).hexdigest()!=digest:raise RuntimeError('Snapshot checksum mismatch')
    lines.append(f'MONITOR_{name}_JAR=./{target.relative_to(ROOT)}')
(DIRECTORY/'jars.env').write_text('\n'.join(lines)+'\n')
