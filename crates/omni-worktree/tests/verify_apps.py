"""Optional offline smoke test; requires Cargo, Flutter, and populated caches."""
import argparse, json, pathlib, subprocess, tempfile
from urllib.parse import urljoin
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--binary', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[1] / 'target/debug/omni-worktree')
binary = parser.parse_args().binary.resolve()
def run(args, cwd):
    p = subprocess.run(args, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if p.returncode: raise RuntimeError(f'{args}:\n{p.stdout}')
    return p.stdout

def init(root):
    root.mkdir()
    run(['git','init','-b','main'],root)
    run(['git','config','user.email','test@example.test'],root)
    run(['git','config','user.name','Test'],root)

def commit(root):
    run(['git','add','.'],root); run(['git','commit','-m','fixture'],root)

def create(root):
    return pathlib.Path(json.loads(run([str(binary),'--repo',str(root),'create','feature'],root))['path'])

with tempfile.TemporaryDirectory(prefix='omni-real-apps-') as tmp:
    base=pathlib.Path(tmp)
    rust=base/'rust'; init(rust)
    (rust/'Cargo.toml').write_text('[package]\nname="worktree-fixture"\nversion="0.1.0"\nedition="2024"\n[dependencies]\nitoa="1"\n')
    (rust/'src').mkdir(); (rust/'src/main.rs').write_text('fn main() { println!("{}", itoa::Buffer::new().format(42)); }\n')
    (rust/'.gitignore').write_text('/target\n')
    run(['cargo','run','--offline','--quiet'],rust); commit(rust)
    source_meta=json.loads(run(['cargo','metadata','--offline','--format-version','1'],rust))
    wt=create(rust)
    assert run(['cargo','run','--offline','--quiet'],wt).strip() == '42'
    target_meta=json.loads(run(['cargo','metadata','--offline','--format-version','1'],wt))
    a=next(p['manifest_path'] for p in source_meta['packages'] if p['name']=='itoa')
    b=next(p['manifest_path'] for p in target_meta['packages'] if p['name']=='itoa')
    assert a == b
    assert source_meta['target_directory'] != target_meta['target_directory']
    (wt/'feature.txt').write_text('completed')
    commit(wt)
    run([str(binary),'--repo',str(rust),'merge','feature'],rust)
    assert not wt.exists()
    print('PASS Rust: both binaries run, identical registry dependency source, isolated build targets',flush=True)

    flutter=base/'flutter'; init(flutter)
    (flutter/'pubspec.yaml').write_text('name: worktree_fixture\nenvironment:\n  sdk: ">=3.0.0 <4.0.0"\ndependencies:\n  flutter:\n    sdk: flutter\n')
    (flutter/'lib').mkdir(); (flutter/'lib/main.dart').write_text("import 'package:flutter/widgets.dart';\nvoid main() => runApp(const SizedBox());\n")
    (flutter/'.gitignore').write_text('.dart_tool/\nbuild/\n.flutter-plugins-dependencies\n')
    run(['flutter','pub','get','--offline'],flutter); commit(flutter)
    wt=create(flutter)
    source_config=json.loads((flutter/'.dart_tool/package_config.json').read_text())
    config=json.loads((wt/'.dart_tool/package_config.json').read_text())
    local=next(p for p in config['packages'] if p['name']=='worktree_fixture')
    assert pathlib.Path(local['rootUri'].replace('file://','').rstrip('/')) == wt
    shared=next(p for p in config['packages'] if p['name']=='flutter')
    original=next(p for p in source_config['packages'] if p['name']=='flutter')
    assert shared['rootUri'].rstrip('/') == urljoin((flutter/'.dart_tool/package_config.json').as_uri(), original['rootUri']).rstrip('/')
    result=run(['flutter','analyze','--no-pub'],wt)
    assert 'No issues found' in result, result
    (wt/'feature.txt').write_text('completed')
    commit(wt)
    run([str(binary),'--repo',str(flutter),'merge','feature'],flutter)
    assert not wt.exists()
    print('PASS Flutter: shared SDK/cache, remapped local package, flutter analyze --no-pub passes',flush=True)
