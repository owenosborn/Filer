#!/usr/bin/env python3
"""
Simple Flask web viewer for the file storage system.
"""

from flask import Flask, render_template, send_file, jsonify, request
from pathlib import Path
import mimetypes
from filer import FileStore

app = Flask(__name__)
store = FileStore()

@app.route('/')
def index():
    """Main page - file browser."""
    return render_template('index.html')

@app.route('/api/files')
def api_files():
    """API endpoint to get all files."""
    tag = request.args.get('tag')
    source = request.args.get('source')
    
    import sqlite3
    import json
    
    conn = sqlite3.connect(store.db_path)
    
    query = """
        SELECT hash, original_paths, size, mime_type, original_filename, 
               tags, created_at 
        FROM files WHERE 1=1
    """
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
    
    files = []
    for hash_val, paths_json, size, mime_type, filename, tags_json, created_at in results:
        paths = json.loads(paths_json)
        files.append({
            'hash': hash_val,
            'hash_short': hash_val[:8],
            'size': size,
            'size_mb': round(size / (1024**2), 2),
            'locations': paths,
            'location_count': len(paths),
            'mime_type': mime_type,
            'filename': filename,
            'tags': json.loads(tags_json) if tags_json else [],
            'created_at': created_at
        })
    
    return jsonify(files)

@app.route('/api/stats')
def api_stats():
    """API endpoint for statistics."""
    import sqlite3
    import json
    
    conn = sqlite3.connect(store.db_path)
    total = conn.execute("SELECT COUNT(*), SUM(size) FROM files").fetchone()
    all_files = conn.execute("SELECT original_paths, tags FROM files").fetchall()
    conn.close()
    
    # Count sources and tags
    source_counts = {}
    tag_counts = {}
    total_locations = 0
    
    for paths_json, tags_json in all_files:
        paths = json.loads(paths_json)
        total_locations += len(paths)
        for p in paths:
            source = p.get("source", "unknown")
            source_counts[source] = source_counts.get(source, 0) + 1
        
        if tags_json:
            tags = json.loads(tags_json)
            for tag in tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
    
    return jsonify({
        'unique_files': total[0],
        'total_locations': total_locations,
        'total_size_gb': round(total[1] / (1024**3), 2),
        'sources': source_counts,
        'tags': dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:20])
    })

@app.route('/file/<hash>')
def view_file(hash):
    """Serve the actual file."""
    file_path = store.get_storage_path(hash)
    
    if not file_path.exists():
        return "File not found", 404
    
    # Guess MIME type
    mime_type, _ = mimetypes.guess_type(str(file_path))
    
    return send_file(file_path, mimetype=mime_type)

@app.route('/api/file/<hash>/info')
def file_info(hash):
    """Get detailed file info."""
    import sqlite3
    import json
    
    conn = sqlite3.connect(store.db_path)
    cursor = conn.execute("""
        SELECT hash, size, mime_type, file_extension, original_filename,
               created_at, modified_at, imported_at, local_path, 
               original_paths, tags, metadata
        FROM files WHERE hash = ?
    """, (hash,))
    
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        return jsonify({'error': 'File not found'}), 404
    
    (hash_val, size, mime_type, ext, filename, created, modified, 
     imported, local, paths_json, tags_json, meta_json) = result
    
    return jsonify({
        'hash': hash_val,
        'size': size,
        'size_mb': round(size / (1024**2), 2),
        'mime_type': mime_type,
        'extension': ext,
        'filename': filename,
        'created_at': created,
        'modified_at': modified,
        'imported_at': imported,
        'local_path': local,
        'locations': json.loads(paths_json),
        'tags': json.loads(tags_json) if tags_json else [],
        'metadata': json.loads(meta_json) if meta_json else {}
    })

if __name__ == '__main__':
    app.run(debug=True, port=5005)
