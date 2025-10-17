#!/usr/bin/env python3
"""
Simple file ingestion script for content-addressable file storage system.
"""

import sqlite3
import hashlib
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional


class FileStore:
    def __init__(self, db_path: str = "filedb.db", storage_root: str = "storage"):
        self.db_path = Path(db_path)
        self.storage_root = Path(storage_root)
        self.storage_root.mkdir(exist_ok=True)
        self.init_database()
    
    def init_database(self):
        """Initialize the SQLite database with schema."""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                hash TEXT PRIMARY KEY,
                size INTEGER NOT NULL,
                mime_type TEXT,
                file_extension TEXT,
                original_filename TEXT,
                created_at TIMESTAMP,
                modified_at TIMESTAMP,
                imported_at TIMESTAMP NOT NULL,
                
                local_path TEXT,
                s3_url TEXT,
                
                original_paths TEXT NOT NULL,
                
                tags TEXT,
                metadata TEXT
            )
        """)
        conn.commit()
        conn.close()
        print(f"Database initialized at {self.db_path}")
    
    def hash_file(self, filepath: Path) -> str:
        """Calculate SHA-256 hash of file."""
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def get_storage_path(self, file_hash: str) -> Path:
        """Convert hash to sharded storage path."""
        return self.storage_root / file_hash[:2] / file_hash[2:4] / file_hash
    
    def extract_path_tags(self, filepath: Path) -> list[str]:
        """Extract directory components as tags."""
        parts = filepath.parts[:-1]  # exclude filename
        # Filter out common root directories
        return [p for p in parts if p not in ('/', '.', '..')]
    
    def ingest_file(self, filepath: Path, source: str = "local") -> dict:
        """
        Ingest a file into the system.
        
        Args:
            filepath: Path to file to ingest
            source: Source identifier (e.g., "Dropbox", "iCloud", "OldMacDrive")
            
        Returns:
            Dict with ingestion results
        """
        if not filepath.exists():
            return {"status": "error", "message": "File not found"}
        
        if not filepath.is_file():
            return {"status": "error", "message": "Not a file"}
        
        # Get file stats
        stat = filepath.stat()
        
        # Calculate hash
        print(f"Hashing {filepath.name}...", end=" ")
        file_hash = self.hash_file(filepath)
        print(f"[{file_hash[:8]}...]")
        
        # Check if already exists
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT hash, original_paths FROM files WHERE hash = ?", 
                            (file_hash,))
        existing = cursor.fetchone()
        
        if existing:
            # File exists - add this as an alternate location
            existing_paths = json.loads(existing[1])
            new_location = {
                "path": str(filepath),
                "source": source,
                "discovered_at": datetime.now().isoformat()
            }
            
            # Check if this exact path already recorded
            if any(p["path"] == str(filepath) for p in existing_paths):
                print(f"  → Already exists (exact path already recorded)")
                conn.close()
                return {
                    "status": "duplicate",
                    "hash": file_hash,
                    "paths": existing_paths
                }
            
            # Add new location
            existing_paths.append(new_location)
            conn.execute(
                "UPDATE files SET original_paths = ? WHERE hash = ?",
                (json.dumps(existing_paths), file_hash)
            )
            conn.commit()
            conn.close()
            
            print(f"  → Added as alternate location (total locations: {len(existing_paths)})")
            return {
                "status": "alternate_location",
                "hash": file_hash,
                "paths": existing_paths
            }
        
        # Get storage path and create directories
        storage_path = self.get_storage_path(file_hash)
        storage_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file to storage (preserves timestamps)
        print(f"  → Copying to {storage_path.relative_to(self.storage_root)}")
        shutil.copy2(filepath, storage_path)
        
        # Extract tags from path
        tags = self.extract_path_tags(filepath)
        
        # Extract file extension
        file_extension = filepath.suffix.lower() if filepath.suffix else ""
        
        # Prepare metadata - store paths as array
        original_paths = [{
            "path": str(filepath),
            "source": source,
            "discovered_at": datetime.now().isoformat()
        }]
        
        # Insert into database
        conn.execute("""
            INSERT INTO files (
                hash, size, file_extension, original_filename,
                created_at, modified_at, imported_at,
                local_path, original_paths, tags, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file_hash,
            stat.st_size,
            file_extension,
            filepath.name,
            datetime.fromtimestamp(stat.st_ctime),
            datetime.fromtimestamp(stat.st_mtime),
            datetime.now(),
            str(storage_path),
            json.dumps(original_paths),
            json.dumps(tags),
            json.dumps({})
        ))
        conn.commit()
        conn.close()
        
        print(f"  ✓ Ingested successfully")
        return {
            "status": "success",
            "hash": file_hash,
            "storage_path": str(storage_path)
        }
    
    def ingest_directory(self, dirpath: Path, source: str = "local", 
                        recursive: bool = True):
        """
        Ingest all files in a directory.
        
        Args:
            dirpath: Directory to ingest
            source: Source identifier
            recursive: Whether to recurse into subdirectories
        """
        dirpath = Path(dirpath)
        if not dirpath.is_dir():
            print(f"Error: {dirpath} is not a directory")
            return
        
        pattern = "**/*" if recursive else "*"
        files = [f for f in dirpath.glob(pattern) if f.is_file()]
        
        print(f"\nIngesting {len(files)} files from {dirpath}")
        print("=" * 60)
        
        stats = {"success": 0, "duplicate": 0, "alternate_location": 0, "error": 0}
        
        for filepath in files:
            result = self.ingest_file(filepath, source)
            stats[result["status"]] += 1
        
        print("\n" + "=" * 60)
        print(f"Summary: {stats['success']} new, {stats['alternate_location']} alternate locations, " +
              f"{stats['duplicate']} exact duplicates, {stats['error']} errors")
    
    def search(self, tag: Optional[str] = None, source: Optional[str] = None):
        """Simple search function."""
        conn = sqlite3.connect(self.db_path)
        
        query = "SELECT hash, original_paths, size FROM files WHERE 1=1"
        params = []
        
        if tag:
            query += " AND tags LIKE ?"
            params.append(f'%{tag}%')
        
        if source:
            query += " AND original_paths LIKE ?"
            params.append(f'%{source}%')
        
        cursor = conn.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Parse paths for display
        parsed_results = []
        for hash_val, paths_json, size in results:
            paths = json.loads(paths_json)
            parsed_results.append((hash_val, paths, size))
        
        return parsed_results
    
    def stats(self):
        """Print database statistics."""
        conn = sqlite3.connect(self.db_path)
        
        total = conn.execute("SELECT COUNT(*), SUM(size) FROM files").fetchone()
        all_files = conn.execute("SELECT original_paths FROM files").fetchall()
        
        conn.close()
        
        # Count sources and locations
        source_counts = {}
        total_locations = 0
        for (paths_json,) in all_files:
            paths = json.loads(paths_json)
            total_locations += len(paths)
            for p in paths:
                source = p.get("source", "unknown")
                source_counts[source] = source_counts.get(source, 0) + 1
        
        print(f"\nFile Store Statistics")
        print("=" * 60)
        print(f"Unique files: {total[0]:,}")
        print(f"Total locations: {total_locations:,}")
        print(f"Total size: {total[1] / (1024**3):.2f} GB")
        print(f"\nBy source:")
        for source, count in sorted(source_counts.items()):
            print(f"  {source}: {count:,} instances")


def main():
    """Example usage."""
    store = FileStore()
    
    # Example: ingest a directory
    store.ingest_directory(Path("./test_files"), source="TestData", recursive=True)
    
    # Show stats
    store.stats()


if __name__ == "__main__":
    main()