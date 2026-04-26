# =============================================================================
# Fraud Intelligence Orchestrator
# =============================================================================
# Runs all 7 pipelines in sequence, then the fraud detector.
#
# Usage:
#   python orchestrator.py
#
# Configuration — edit the section below before running:
#   BASE_DIR        : path to your fraud_project folder
#   FORCE_RERUN     : True = always re-run; False = skip if output exists
#   PIPELINES_TO_RUN: set any source to False to skip it
# =============================================================================

import os
import sys
import time
import subprocess

# ── ① CONFIGURATION ──────────────────────────────────────────────────────────
# Set FRAUD_BASE_DIR as an environment variable, or update the fallback below.
# This is the ONLY line you need to change.
BASE_DIR = os.environ.get(
    'FRAUD_BASE_DIR',
    '/Users/josephsingleton/Documents/fraud-dashboard'   # <-- update this
)

# Inject so all child scripts inherit it
os.environ['FRAUD_BASE_DIR'] = BASE_DIR

OUTPUT_FOLDER = os.path.join(BASE_DIR, 'outputs')
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# True  → always re-run, even if output file already exists
# False → skip a pipeline if its output file already exists (faster)
FORCE_RERUN = True

# Toggle individual pipelines on/off
PIPELINES_TO_RUN = {
    'fincen':           False,
    'ftc':              False,
    'fbi':              False,
    'ic3':              False,
    'bleepingcomputer': False,
    'outseer':          False,
    'pymnts':           False,
}

# Per-pipeline timeouts in seconds
# FBI (~37 min for 1500 Wayback fetches) and BleepingComputer need extended time
PIPELINE_TIMEOUTS = {
    'fincen':           1800,
    'ftc':              1800,
    'fbi':              7200,
    'ic3':              1800,
    'bleepingcomputer': 3600,
    'outseer':          1800,
    'pymnts':           1800,
}

# Script filenames — must be in BASE_DIR alongside this orchestrator
PIPELINE_SCRIPTS = {
    'fincen':           'FinCen.py',
    'ftc':              'FTC.py',
    'fbi':              'FBI.py',
    'ic3':              'IC3.py',
    'bleepingcomputer': 'BleepingComputer.py',
    'outseer':          'Outseer.py',
    'pymnts':           'PYMNTS.py',
}

# Expected output files — used to check whether a pipeline needs to run
EXPECTED_OUTPUTS = {
    'fincen':           os.path.join(OUTPUT_FOLDER, 'fincen_tagged_chunks.jsonl'),
    'ftc':              os.path.join(OUTPUT_FOLDER, 'ftc_master.jsonl'),
    'fbi':              os.path.join(OUTPUT_FOLDER, 'fbi_tagged_chunks.jsonl'),
    'ic3':              os.path.join(OUTPUT_FOLDER, 'ic3_tagged_chunks.jsonl'),
    'bleepingcomputer': os.path.join(OUTPUT_FOLDER, 'bleepingcomputer_fraud_data.csv'),
    'outseer':          os.path.join(OUTPUT_FOLDER, 'outseer_scraped_data.jsonl'),
    'pymnts':           os.path.join(OUTPUT_FOLDER, 'pymnts_master.jsonl'),
}

FRAUD_DETECTOR_SCRIPT = 'fraud_detector.ipynb'  # still a notebook — run separately

# ── ② HELPERS ─────────────────────────────────────────────────────────────────

def run_pipeline(name: str) -> tuple[bool, float, str]:
    """
    Run a pipeline script as a subprocess.
    Returns (success, elapsed_seconds, error_message).
    """
    script_path = os.path.join(BASE_DIR, PIPELINE_SCRIPTS[name])
    timeout     = PIPELINE_TIMEOUTS.get(name, 1800)
    start       = time.time()

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=os.environ.copy(),   # passes FRAUD_BASE_DIR to child
        )
        elapsed = round(time.time() - start, 1)

        if result.returncode != 0:
            # Show last 2000 chars of stderr so it fits in the terminal
            return False, elapsed, result.stderr[-2000:]

        return True, elapsed, None

    except subprocess.TimeoutExpired:
        elapsed = round(time.time() - start, 1)
        return False, elapsed, f'TIMEOUT after {timeout}s'

    except Exception as e:
        elapsed = round(time.time() - start, 1)
        return False, elapsed, str(e)


# ── ③ PRE-FLIGHT CHECK ────────────────────────────────────────────────────────

print('=' * 60)
print('FRAUD INTELLIGENCE ORCHESTRATOR')
print('=' * 60)
print(f'BASE_DIR:    {BASE_DIR}')
print(f'FORCE_RERUN: {FORCE_RERUN}')
print()
print('PRE-FLIGHT CHECK')
print('-' * 60)

run_queue    = []
skip_queue   = []
missing_scripts = []

for name in PIPELINE_SCRIPTS:
    enabled     = PIPELINES_TO_RUN.get(name, True)
    script_path = os.path.join(BASE_DIR, PIPELINE_SCRIPTS[name])
    output_path = EXPECTED_OUTPUTS[name]
    script_exists = os.path.exists(script_path)
    out_exists    = os.path.exists(output_path)

    if not enabled:
        print(f'  ⏭  {name:<20} SKIPPED (disabled)')
        skip_queue.append(name)
        continue

    if not script_exists:
        print(f'  ✗  {name:<20} SCRIPT NOT FOUND: {PIPELINE_SCRIPTS[name]}')
        missing_scripts.append(name)
        continue

    if out_exists and not FORCE_RERUN:
        size_kb = round(os.path.getsize(output_path) / 1024, 1)
        print(f'  ✓  {name:<20} Output exists ({size_kb} KB) — skipping')
        skip_queue.append(name)
    else:
        reason = 'FORCE_RERUN=True' if FORCE_RERUN else 'no output yet'
        print(f'  →  {name:<20} Will RUN ({reason})')
        run_queue.append(name)

print()
print(f'Pipelines to run:  {len(run_queue)} → {run_queue}')
print(f'Pipelines to skip: {len(skip_queue)}')

if missing_scripts:
    print(f'\n  Missing scripts — fix before continuing:')
    for s in missing_scripts:
        print(f'    {PIPELINE_SCRIPTS[s]} (expected in {BASE_DIR})')
    print()

if not run_queue:
    print('\nNothing to run. Set FORCE_RERUN=True to re-run everything.')
    sys.exit(0)

# ── ④ RUN PIPELINES ───────────────────────────────────────────────────────────

print()
print('=' * 60)
print('RUNNING PIPELINES')
print('=' * 60)

pipeline_results = {}

for name in run_queue:
    out_path = EXPECTED_OUTPUTS[name]

    print(f'\n{"-" * 55}')
    print(f'  {name.upper()} — {PIPELINE_SCRIPTS[name]}  (timeout={PIPELINE_TIMEOUTS.get(name, 1800)}s)')
    print(f'{"-" * 55}')

    success, elapsed, error = run_pipeline(name)

    if success:
        out_exists = os.path.exists(out_path)
        if out_exists:
            size_kb = round(os.path.getsize(out_path) / 1024, 1)
            print(f'  ✓ Success ({elapsed}s) — {os.path.basename(out_path)} ({size_kb} KB)')
            pipeline_results[name] = 'success'
        else:
            print(f'  ⚠ Script ran but output not found: {out_path}')
            print(f'    Check the script for wrong save path.')
            pipeline_results[name] = 'no_output'
    else:
        print(f'  ✗ FAILED ({elapsed}s)')
        print(f'  Error:\n{error}')
        pipeline_results[name] = 'failed'

# ── ⑤ FINAL STATUS REPORT ────────────────────────────────────────────────────

print()
print('=' * 60)
print('PIPELINE SUMMARY')
print('=' * 60)

all_sources = list(EXPECTED_OUTPUTS.keys())
ready_sources = []

for name in all_sources:
    out_path   = EXPECTED_OUTPUTS[name]
    exists     = os.path.exists(out_path)
    size_kb    = round(os.path.getsize(out_path) / 1024, 1) if exists else 0
    run_result = pipeline_results.get(name, 'skipped')

    if exists:
        icon = '✓'
        note = f'{size_kb} KB'
        ready_sources.append(name)
    elif run_result == 'failed':
        icon = '✗'
        note = 'FAILED — check errors above'
    elif not PIPELINES_TO_RUN.get(name, True):
        icon = '⏭'
        note = 'Disabled'
    else:
        icon = '⚠'
        note = 'No output produced'

    print(f'  {icon}  {name:<20} {note}')

print()
print(f'Sources ready for detector: {len(ready_sources)} / {len(all_sources)}')
print()

#if ready_sources:
    ##print('Next step: run the fraud detector notebook.')
    #print('Next step: run the fraud detector.')
    ##print(f'  Open {FRAUD_DETECTOR_SCRIPT} in VS Code and run all cells.')
    #print(f'  Outputs will be read from: {OUTPUT_FOLDER}')
    #print(f'  python {FRAUD_DETECTOR_SCRIPT}')
#else:
    #print('No outputs available — fix pipeline errors above before running the detector.')


if ready_sources:
    print('Running fraud detector...')
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, os.path.join(BASE_DIR, FRAUD_DETECTOR_SCRIPT)],
        env=os.environ.copy()
    )
    if result.returncode == 0:
        print('Fraud detector complete.')
    else:
        print('Fraud detector failed — check errors above.')