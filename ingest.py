#!/usr/bin/env python3
"""
Simple file ingestion tool with interactive prompts.
Usage: python ingest.py path/to/files
"""

import sys
from pathlib import Path
from filer import FileStore


def count_files(path: Path, recursive: bool = True) -> list[Path]:
    """Count and return list of files in path."""
    if path.is_file():
        return [path]
    
    pattern = "**/*" if recursive else "*"
    return [f for f in path.glob(pattern) if f.is_file()]


def print_progress(current: int, total: int, stats: dict, width: int = 50):
    """Print progress bar with stats (overwrites same line)."""
    percent = current / total if total > 0 else 0
    filled = int(width * percent)
    bar = '█' * filled + '░' * (width - filled)
    
    # \r returns to start of line, end='' prevents newline
    print(f"\r[{bar}] {current}/{total} | "
          f"New: {stats['success']} | "
          f"Alt: {stats['alternate_location']} | "
          f"Dup: {stats['duplicate']} | "
          f"Err: {stats['error']}", 
          end='', flush=True)


def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest.py <path>")
        print("       python ingest.py path/to/files")
        sys.exit(1)
    
    path = Path(sys.argv[1])
    
    if not path.exists():
        print(f"Error: {path} does not exist")
        sys.exit(1)
    
    # Count files first
    print("Scanning for files...")
    files = count_files(path)
    
    if not files:
        print("No files found!")
        sys.exit(1)
    
    # Confirm ingestion
    location_desc = f"file" if len(files) == 1 else f"directory containing {len(files)} files"
    confirm = input(f"\nIngest {location_desc} from '{path}'? (y/n): ").lower().strip()
    
    if confirm != 'y':
        print("Cancelled.")
        sys.exit(0)
    
    # Get source
    source = input("Enter source (e.g., 'Dropbox', 'OldMacDrive'): ").strip()
    if not source:
        source = "local"
    
    # Get additional tags
    tags_input = input("Enter additional tags (comma separated, optional): ").strip()
    extra_tags = [t.strip() for t in tags_input.split(',') if t.strip()]
    
    # Initialize store (verbose=False for clean progress display)
    store = FileStore(verbose=False)
    
    # Ingest with progress
    print(f"\n{'='*60}")
    print(f"Ingesting {len(files)} files from: {path}")
    print(f"Source: {source}")
    if extra_tags:
        print(f"Tags: {', '.join(extra_tags)}")
    print(f"{'='*60}\n")
    
    stats = {"success": 0, "duplicate": 0, "alternate_location": 0, "error": 0}
    
    for i, filepath in enumerate(files, 1):
        result = store.ingest_file(filepath, source, additional_tags=extra_tags)
        stats[result["status"]] += 1
        print_progress(i, len(files), stats)
    
    # Final newline and summary
    print("\n" + "=" * 60)
    print(f"✓ Complete!")
    print(f"  {stats['success']} new files")
    print(f"  {stats['alternate_location']} alternate locations")
    print(f"  {stats['duplicate']} exact duplicates")
    if stats['error'] > 0:
        print(f"  {stats['error']} errors")


if __name__ == "__main__":
    main()