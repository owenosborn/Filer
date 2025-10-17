#!/usr/bin/env python3
"""
Command-line interface for the file storage system.
"""

import argparse
import sys
import json
from pathlib import Path
from filer import FileStore


def cmd_ingest(args, store):
    """Ingest files into the system."""
    path = Path(args.path)
    
    if not path.exists():
        print(f"Error: {path} does not exist")
        return 1
    
    if path.is_file():
        result = store.ingest_file(path, source=args.source, create_sidecar=args.sidecar)
        if result["status"] == "error":
            print(f"Error: {result['message']}")
            return 1
    elif path.is_dir():
        store.ingest_directory(path, source=args.source, recursive=args.recursive)
    else:
        print(f"Error: {path} is neither a file nor directory")
        return 1
    
    return 0


def cmd_search(args, store):
    """Search for files."""
    results = store.search(tag=args.tag, source=args.source)
    
    if not results:
        print("No files found")
        return 0
    
    print(f"\nFound {len(results)} file(s):\n")
    for hash_val, paths, size in results:
        size_mb = size / (1024**2)
        print(f"Hash: {hash_val}")
        print(f"Size: {size_mb:.2f} MB")
        print(f"Locations ({len(paths)}):")
        for p in paths:
            print(f"  - {p['path']}")
            print(f"    Source: {p['source']}, Found: {p['discovered_at']}")
        print()
    
    return 0


def cmd_list(args, store):
    """List all files."""
    results = store.search()  # No filters = all files
    
    if not results:
        print("No files in database")
        return 0
    
    # Sort by size if requested
    if args.sort == "size":
        results.sort(key=lambda x: x[2], reverse=True)
    
    # Apply limit
    if args.limit:
        results = results[:args.limit]
    
    print(f"\nListing {len(results)} file(s):\n")
    print(f"{'Hash':<12} {'Size':>10} {'Locations':>10} {'First Path'}")
    print("-" * 80)
    
    for hash_val, paths, size in results:
        size_mb = size / (1024**2)
        first_path = Path(paths[0]['path']).name if paths else "N/A"
        print(f"{hash_val[:10]}.. {size_mb:>9.2f}M {len(paths):>10} {first_path}")
    
    return 0


def cmd_info(args, store):
    """Show detailed info for a specific file."""
    import sqlite3
    
    conn = sqlite3.connect(store.db_path)
    cursor = conn.execute("""
        SELECT hash, size, mime_type, created_at, imported_at, 
               local_path, original_paths, tags, metadata
        FROM files WHERE hash = ? OR hash LIKE ?
    """, (args.hash, f"{args.hash}%"))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print(f"No file found with hash matching: {args.hash}")
        return 1
    
    hash_val, size, mime, created, imported, local, paths_json, tags_json, meta_json = result
    
    print(f"\nFile Information:")
    print("=" * 60)
    print(f"Hash:         {hash_val}")
    print(f"Size:         {size / (1024**2):.2f} MB ({size:,} bytes)")
    print(f"MIME Type:    {mime or 'Not detected'}")
    print(f"Created:      {created}")
    print(f"Imported:     {imported}")
    print(f"Local Path:   {local}")
    
    paths = json.loads(paths_json)
    print(f"\nLocations ({len(paths)}):")
    for p in paths:
        print(f"  {p['path']}")
        print(f"    Source: {p['source']}, Discovered: {p['discovered_at']}")
    
    tags = json.loads(tags_json) if tags_json else []
    if tags:
        print(f"\nTags: {', '.join(tags)}")
    
    metadata = json.loads(meta_json) if meta_json else {}
    if metadata:
        print(f"\nMetadata:")
        print(json.dumps(metadata, indent=2))
    
    return 0


def cmd_stats(args, store):
    """Show database statistics."""
    store.stats()
    return 0


def cmd_locate(args, store):
    """Show all locations for a file by hash."""
    import sqlite3
    
    conn = sqlite3.connect(store.db_path)
    cursor = conn.execute("""
        SELECT hash, original_paths FROM files 
        WHERE hash = ? OR hash LIKE ?
    """, (args.hash, f"{args.hash}%"))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        print(f"No file found with hash matching: {args.hash}")
        return 1
    
    hash_val, paths_json = result
    paths = json.loads(paths_json)
    
    print(f"\nFile: {hash_val}")
    print(f"Found in {len(paths)} location(s):\n")
    
    for i, p in enumerate(paths, 1):
        print(f"{i}. {p['path']}")
        print(f"   Source: {p['source']}")
        print(f"   Discovered: {p['discovered_at']}")
        print()
    
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Content-addressable file storage system",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--db', 
        default='filedb.db',
        help='Path to database file (default: filedb.db)'
    )
    parser.add_argument(
        '--storage',
        default='storage',
        help='Path to storage root (default: storage/)'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Ingest files into storage')
    ingest_parser.add_argument('path', help='File or directory to ingest')
    ingest_parser.add_argument('--source', default='local', help='Source identifier')
    ingest_parser.add_argument('--recursive', '-r', action='store_true', 
                              help='Recursively ingest directories')
    ingest_parser.add_argument('--no-sidecar', dest='sidecar', action='store_false',
                              help='Skip creating sidecar .meta.json files')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search for files')
    search_parser.add_argument('--tag', help='Search by tag')
    search_parser.add_argument('--source', help='Search by source')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all files')
    list_parser.add_argument('--limit', type=int, help='Limit number of results')
    list_parser.add_argument('--sort', choices=['size'], help='Sort results')
    
    # Info command
    info_parser = subparsers.add_parser('info', help='Show detailed file info')
    info_parser.add_argument('hash', help='File hash (full or prefix)')
    
    # Locate command
    locate_parser = subparsers.add_parser('locate', help='Show all locations for a file')
    locate_parser.add_argument('hash', help='File hash (full or prefix)')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show database statistics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Initialize FileStore
    store = FileStore(db_path=args.db, storage_root=args.storage)
    
    # Dispatch to command handlers
    commands = {
        'ingest': cmd_ingest,
        'search': cmd_search,
        'list': cmd_list,
        'info': cmd_info,
        'locate': cmd_locate,
        'stats': cmd_stats,
    }
    
    return commands[args.command](args, store)


if __name__ == "__main__":
    sys.exit(main())
