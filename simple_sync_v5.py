#!/usr/bin/env python3
"""
Simplified sync system for Homely Copier v4.
Shows hash calculation phase and improved speed display.
"""

import os
import sys
import time
import hashlib
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set, Dict, Optional, Tuple, List
from dataclasses import dataclass, field
import threading
from datetime import datetime, timedelta
import argparse
from collections import defaultdict
import queue

import yaml
from rich.console import Console
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent


@dataclass
class FileTask:
    """Represents a file being processed."""
    name: str
    total_size: int
    bytes_done: int = 0
    start_time: float = field(default_factory=time.time)
    status: str = "waiting"  # waiting, hashing, caching, uploading, complete
    speed: float = 0.0
    
    @property
    def progress(self) -> float:
        return (self.bytes_done / self.total_size * 100) if self.total_size > 0 else 0
    
    @property
    def duration(self) -> str:
        elapsed = time.time() - self.start_time
        return str(timedelta(seconds=int(elapsed)))
    
    def update(self, bytes_done: int):
        self.bytes_done = bytes_done
        elapsed = time.time() - self.start_time
        if elapsed > 0:
            self.speed = bytes_done / elapsed / (1024 * 1024)  # MB/s


class ProgressDisplay:
    """Custom progress display with active files at top."""
    
    def __init__(self):
        self.console = Console()
        self.tasks: Dict[str, FileTask] = {}
        self.lock = threading.Lock()
        self.stats = {
            'phase': 'idle',
            'hashed_count': 0,
            'hashed_size': 0,
            'cached_count': 0,
            'cached_size': 0,
            'uploaded_count': 0,
            'uploaded_size': 0,
            'total_count': 0,
            'total_size': 0,
            'start_time': time.time(),
            'scan_status': ''
        }
    
    def add_task(self, task_id: str, name: str, size: int, status: str = "waiting") -> None:
        with self.lock:
            self.tasks[task_id] = FileTask(name, size, status=status)
    
    def update_task(self, task_id: str, bytes_done: int, status: str = None) -> None:
        with self.lock:
            if task_id in self.tasks:
                self.tasks[task_id].update(bytes_done)
                if status:
                    self.tasks[task_id].status = status
    
    def complete_task(self, task_id: str) -> None:
        with self.lock:
            if task_id in self.tasks:
                task = self.tasks[task_id]
                task.status = "complete"
                task.bytes_done = task.total_size
                
                if self.stats['phase'] == 'hashing':
                    self.stats['hashed_count'] += 1
                    self.stats['hashed_size'] += task.total_size
                elif self.stats['phase'] == 'caching':
                    self.stats['cached_count'] += 1
                    self.stats['cached_size'] += task.total_size
                elif self.stats['phase'] == 'uploading':
                    self.stats['uploaded_count'] += 1
                    self.stats['uploaded_size'] += task.total_size
    
    def remove_task(self, task_id: str) -> None:
        with self.lock:
            if task_id in self.tasks:
                del self.tasks[task_id]
    
    def set_phase(self, phase: str) -> None:
        with self.lock:
            self.stats['phase'] = phase
            # Reset counts when changing phase
            if phase == 'hashing':
                self.stats['hashed_count'] = 0
                self.stats['hashed_size'] = 0
    
    def set_scan_status(self, status: str) -> None:
        with self.lock:
            self.stats['scan_status'] = status
    
    def set_total(self, count: int, size: int) -> None:
        with self.lock:
            self.stats['total_count'] = count
            self.stats['total_size'] = size
    
    def create_display(self) -> Layout:
        """Create the display layout."""
        layout = Layout()
        
        with self.lock:
            header = self._create_header()
            table = self._create_file_table()
            
            layout.split_column(
                Layout(header, size=7),
                Layout(Panel(table, title="File Progress", border_style="blue"))
            )
        
        return layout
    
    def _create_header(self) -> Panel:
        """Create header with status information."""
        elapsed = time.time() - self.stats['start_time']
        duration = str(timedelta(seconds=int(elapsed)))
        
        grid = Table.grid(padding=1)
        grid.add_column(style="bold magenta", justify="right")
        grid.add_column()
        grid.add_column(style="bold magenta", justify="right")
        grid.add_column()
        
        phase_display = {
            'idle': 'Idle',
            'hashing': 'Phase 1: Calculating Hashes',
            'caching': 'Phase 2: Caching Files',
            'uploading': 'Phase 3: Uploading to Output',
            'cleaning': 'Cleaning Up Cache',
            'monitoring': 'Monitoring for Changes'
        }.get(self.stats['phase'], self.stats['phase'])
        
        phase_color = {
            'idle': 'white',
            'hashing': 'cyan',
            'caching': 'yellow',
            'uploading': 'green',
            'cleaning': 'magenta',
            'monitoring': 'blue'
        }.get(self.stats['phase'], 'white')
        
        grid.add_row(
            "Current:", 
            Text(phase_display, style=f"bold {phase_color}"),
            "Duration:", 
            Text(duration, style="cyan")
        )
        
        if self.stats['phase'] == 'hashing':
            grid.add_row(
                "Hashing Status:",
                f"{self.stats['hashed_count']}/{self.stats['total_count']} files "
                f"({self._format_size(self.stats['hashed_size'])}/"
                f"{self._format_size(self.stats['total_size'])})",
                "Speed:",
                self._format_speed(self._get_total_speed())
            )
            if self.stats['scan_status']:
                grid.add_row(
                    "Current:",
                    Text(self.stats['scan_status'], style="dim"),
                    "", ""
                )
        elif self.stats['phase'] == 'caching':
            grid.add_row(
                "Caching Status:",
                f"{self.stats['cached_count']}/{self.stats['total_count']} files "
                f"({self._format_size(self.stats['cached_size'])}/"
                f"{self._format_size(self.stats['total_size'])})",
                "Speed:",
                self._format_speed(self._get_total_speed())
            )
        elif self.stats['phase'] == 'uploading':
            grid.add_row(
                "Upload Status:",
                f"{self.stats['uploaded_count']}/{self.stats['total_count']} files "
                f"({self._format_size(self.stats['uploaded_size'])}/"
                f"{self._format_size(self.stats['total_size'])})",
                "Speed:",
                self._format_speed(self._get_total_speed())
            )
        
        return Panel(grid, title="[bold]Homely Copier Status[/bold]", border_style="cyan")
    
    def _create_file_table(self) -> Table:
        """Create table of files with active ones at top."""
        table = Table(show_header=True, header_style="bold blue", box=None)
        table.add_column("File", style="cyan", no_wrap=True, width=40)
        table.add_column("Progress", justify="center", width=25)
        table.add_column("Status", justify="center", width=12)
        table.add_column("Speed", justify="right", width=12)
        table.add_column("Time", justify="right", width=10)
        
        sorted_tasks = sorted(
            self.tasks.items(),
            key=lambda x: (
                x[1].status == "waiting",
                x[1].status == "complete",
                -x[1].progress
            )
        )
        
        for i, (task_id, task) in enumerate(sorted_tasks[:20]):
            if i >= 15 and task.status == "complete":
                remaining = len([t for t in sorted_tasks[i:] if t[1].status != "complete"])
                if remaining > 0:
                    table.add_row(
                        f"... and {remaining} more files",
                        "", "", "", "",
                        style="dim"
                    )
                break
            
            name = task.name
            if len(name) > 38:
                name = "..." + name[-35:]
            
            progress_bar = self._make_progress_bar(task.progress)
            
            status_colors = {
                "waiting": "dim white",
                "hashing": "cyan",
                "caching": "yellow",
                "uploading": "green",
                "complete": "dim green"
            }
            status = Text(task.status.title(), style=status_colors.get(task.status, "white"))
            
            speed = self._format_speed(task.speed) if task.status in ('hashing', 'caching', 'uploading') else "-"
            
            table.add_row(name, progress_bar, status, speed, task.duration)
        
        return table
    
    def _make_progress_bar(self, progress: float) -> Text:
        """Create a text progress bar."""
        width = 20
        filled = int(progress / 100 * width)
        bar = "█" * filled + "░" * (width - filled)
        
        color = "green" if progress >= 100 else "yellow" if progress > 50 else "blue"
        # Show decimal for progress under 100%
        if progress < 100:
            return Text(f"[{bar}] {progress:>5.1f}%", style=color)
        else:
            return Text(f"[{bar}] {progress:>3.0f}%", style=color)
    
    def _format_size(self, size: int) -> str:
        """Format size in human-readable form."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"
    
    def _format_speed(self, speed_mbps: float) -> str:
        """Format speed with appropriate precision."""
        # Convert MB/s to Mb/s (megabits)
        speed_mbits = speed_mbps * 8
        
        if speed_mbits == 0:
            return "0.000 Mb/s"
        elif speed_mbits < 0.001:
            # Show in b/s for extremely slow speeds
            return f"{speed_mbits * 1000 * 1000:.1f} b/s"
        elif speed_mbits < 1:
            # Show in Kb/s for very slow speeds
            return f"{speed_mbits * 1000:.3f} Kb/s"
        elif speed_mbits < 10:
            # Show 3 decimal places for slow speeds
            return f"{speed_mbits:.3f} Mb/s"
        else:
            # Show 3 decimal places for all speeds for consistency
            return f"{speed_mbits:.3f} Mb/s"
    
    def _get_total_speed(self) -> float:
        """Get total transfer speed across all active tasks."""
        active_tasks = [t for t in self.tasks.values() 
                       if t.status in ('hashing', 'caching', 'uploading')]
        return sum(t.speed for t in active_tasks)


class SyncEventHandler(FileSystemEventHandler):
    """Handle file system events for watch mode."""
    
    def __init__(self, sync_instance):
        super().__init__()
        self.sync = sync_instance
        self.pending_files = defaultdict(dict)  # path -> {time, input_name}
        self.lock = threading.Lock()
        self.last_process_time = 0
        self.debounce_seconds = 2.0
    
    def _should_process_file(self, path: str) -> bool:
        """Check if file should be processed."""
        path_obj = Path(path)
        
        # Skip directories
        if path_obj.is_dir():
            return False
        
        # Skip excluded files
        filename = path_obj.name
        if (filename in self.sync.exclude_files or 
            filename.startswith('.') or 
            filename.endswith('.hash')):
            return False
        
        # Skip excluded directories in path
        for part in path_obj.parts:
            if part in self.sync.exclude_dirs:
                return False
        
        return True
    
    def _get_input_name(self, path: str) -> Optional[str]:
        """Determine which input this file belongs to."""
        path_obj = Path(path).resolve()
        
        for input_name, input_config in self.sync.config['inputs'].items():
            input_path = Path(input_config['path']).resolve()
            try:
                path_obj.relative_to(input_path)
                return input_name
            except ValueError:
                continue
        
        return None
    
    def on_created(self, event: FileSystemEvent) -> None:
        """Handle file creation events."""
        if not self._should_process_file(event.src_path):
            return
        
        input_name = self._get_input_name(event.src_path)
        if input_name:
            with self.lock:
                self.pending_files[event.src_path] = {
                    'time': time.time(),
                    'input_name': input_name
                }
    
    def on_modified(self, event: FileSystemEvent) -> None:
        """Handle file modification events."""
        if not self._should_process_file(event.src_path):
            return
        
        input_name = self._get_input_name(event.src_path)
        if input_name:
            with self.lock:
                self.pending_files[event.src_path] = {
                    'time': time.time(),
                    'input_name': input_name
                }
    
    def get_pending_files(self) -> List[Tuple[str, str]]:
        """Get files that are ready to process (after debounce period)."""
        current_time = time.time()
        ready_files = []
        
        with self.lock:
            files_to_process = []
            
            for path, info in list(self.pending_files.items()):
                if current_time - info['time'] >= self.debounce_seconds:
                    files_to_process.append((path, info['input_name']))
                    del self.pending_files[path]
            
        return files_to_process


class SimpleSync:
    """Simplified sync system without database."""
    
    def __init__(self, config_path: str = "config.yaml"):
        self.console = Console()
        self.config = self._load_config(config_path)
        self.progress = ProgressDisplay()
        
        # Hash caches
        self.hash_cache = {}  # path -> (mtime, hash)
        
        # Exclusion patterns
        self.exclude_dirs = {'.git', '__pycache__', 'node_modules', '.venv', 'venv', 
                            '.idea', '.vscode', 'logs'}
        self.exclude_files = {'.DS_Store', 'Thumbs.db', '*.hash'}
        
        # In-memory tracking
        self.output_hashes: Set[str] = set()
        self.cache_map: Dict[str, str] = {}  # hash -> cache_path
    
    def _load_config(self, path: str) -> dict:
        """Load configuration from YAML."""
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    
    def _get_hash_file_path(self, file_path: str) -> str:
        """Get the path for the .hash file."""
        return f"{file_path}.hash"
    
    def _read_cached_hash(self, file_path: str) -> Optional[str]:
        """Read hash from .hash file if it exists and is valid."""
        hash_file = self._get_hash_file_path(file_path)
        
        try:
            if os.path.exists(hash_file):
                file_mtime = os.path.getmtime(file_path)
                hash_mtime = os.path.getmtime(hash_file)
                
                # If hash file is newer than file, use cached hash
                if hash_mtime >= file_mtime:
                    with open(hash_file, 'r') as f:
                        return f.read().strip()
        except (OSError, IOError):
            pass
        
        return None
    
    def _save_hash(self, file_path: str, file_hash: str) -> None:
        """Save hash to .hash file."""
        hash_file = self._get_hash_file_path(file_path)
        try:
            with open(hash_file, 'w') as f:
                f.write(file_hash)
        except (OSError, IOError):
            pass
    
    def _calculate_hash(self, file_path: str, chunk_size: int = 65536) -> str:
        """Calculate SHA256 hash of a file, using cache if available."""
        # Check in-memory cache first
        try:
            mtime = os.path.getmtime(file_path)
            if file_path in self.hash_cache:
                cached_mtime, cached_hash = self.hash_cache[file_path]
                if cached_mtime == mtime:
                    return cached_hash
        except (OSError, IOError):
            pass
        
        # Check .hash file
        cached_hash = self._read_cached_hash(file_path)
        if cached_hash:
            self.hash_cache[file_path] = (mtime, cached_hash)
            return cached_hash
        
        # Calculate hash
        sha256 = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(chunk_size):
                    sha256.update(chunk)
            
            file_hash = sha256.hexdigest()
            
            # Cache it
            self._save_hash(file_path, file_hash)
            self.hash_cache[file_path] = (mtime, file_hash)
            
            return file_hash
        except (OSError, IOError):
            return ""
    
    def _scan_directory(self, path: str) -> List[Tuple[str, int]]:
        """Scan directory and return list of (file_path, size) tuples."""
        files = []
        
        for root, dirs, filenames in os.walk(path):
            # Filter out excluded directories
            dirs[:] = [d for d in dirs if d not in self.exclude_dirs]
            
            for filename in filenames:
                if (filename in self.exclude_files or 
                    filename.startswith('.') or 
                    filename.endswith('.hash')):
                    continue
                
                file_path = os.path.join(root, filename)
                try:
                    size = os.path.getsize(file_path)
                    files.append((file_path, size))
                except (OSError, IOError):
                    continue
        
        return files
    
    def scan_and_hash_directory(self, directory: str, description: str) -> Dict[str, str]:
        """Scan directory and compute hashes for all files with progress."""
        self.progress.set_scan_status(f"Scanning {description} directory...")
        
        files = self._scan_directory(directory)
        if not files:
            return {}
        
        # Set total for this directory
        total_size = sum(f[1] for f in files)
        self.progress.set_total(len(files), total_size)
        
        hash_map = {}
        
        # Process files with progress
        for i, (file_path, size) in enumerate(files):
            task_id = f"hash_{description}_{i}"
            
            # Create display name
            try:
                relative = Path(file_path).relative_to(directory)
                display_name = f"{description}/{relative}"
            except:
                display_name = Path(file_path).name
            
            # Limit display name length
            if len(display_name) > 50:
                display_name = "..." + display_name[-47:]
            
            # Add task for this file
            self.progress.add_task(task_id, display_name, size, status="hashing")
            
            # Simulate hash calculation as progress
            # Since hashing is fast, we'll show it as instant completion
            file_hash = self._calculate_hash(file_path)
            
            if file_hash:
                hash_map[file_hash] = file_path
            
            # Complete and remove task
            self.progress.complete_task(task_id)
            self.progress.remove_task(task_id)
            
            # Update scan status
            self.progress.set_scan_status(f"Hashed {i+1}/{len(files)} files in {description}")
        
        return hash_map
    
    def phase1_hash_directories(self):
        """Phase 1: Calculate hashes for output and cache directories."""
        self.progress.set_phase('hashing')
        
        # Hash output directory
        output_path = self.config['output']['path']
        if os.path.exists(output_path):
            self.progress.set_scan_status("Scanning output directory...")
            output_map = self.scan_and_hash_directory(output_path, "output")
            self.output_hashes = set(output_map.keys())
            self.console.print(f"[cyan]Found {len(self.output_hashes)} files in output")
        
        # Hash cache directory
        cache_path = self.config['cache']['path']
        if os.path.exists(cache_path):
            self.progress.set_scan_status("Scanning cache directory...")
            self.cache_map = self.scan_and_hash_directory(cache_path, "cache")
            self.console.print(f"[cyan]Found {len(self.cache_map)} files in cache")
        
        self.progress.set_scan_status("")
    
    def phase2_cache_all(self):
        """Phase 2: Cache all files from all inputs."""
        self.progress.set_phase('caching')
        
        # Collect all files that need caching
        files_to_cache = []
        
        for input_name, input_config in self.config['inputs'].items():
            input_path = input_config['path']
            if not os.path.exists(input_path):
                self.console.print(f"[yellow]Warning: {input_path} does not exist")
                continue
            
            files = self._scan_directory(input_path)
            
            for file_path, size in files:
                file_hash = self._calculate_hash(file_path)
                if not file_hash:
                    continue
                
                # Skip if already in output or cache
                if file_hash in self.output_hashes or file_hash in self.cache_map:
                    continue
                
                # Determine cache path
                base_path = Path(input_config['path'])
                relative_path = Path(file_path).relative_to(base_path)
                cache_path = Path(self.config['cache']['path']) / input_name / relative_path
                
                files_to_cache.append((file_path, str(cache_path), size, input_name, file_hash))
        
        if not files_to_cache:
            return
        
        # Set total for progress
        total_size = sum(f[2] for f in files_to_cache)
        self.progress.set_total(len(files_to_cache), total_size)
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {}
            
            for i, (src, dst, size, input_name, file_hash) in enumerate(files_to_cache):
                task_id = f"cache_{i}"
                relative = Path(src).relative_to(self.config['inputs'][input_name]['path'])
                display_name = f"{input_name}/{relative}"
                
                self.progress.add_task(task_id, display_name, size, status="caching")
                
                future = executor.submit(
                    self.copy_file_with_progress, src, dst, task_id, False
                )
                futures[future] = (task_id, file_hash, dst)
            
            for future in as_completed(futures):
                task_id, file_hash, cache_path = futures[future]
                try:
                    if future.result():
                        self.progress.complete_task(task_id)
                        self.cache_map[file_hash] = cache_path
                        # Remove after delay
                        threading.Timer(3.0, lambda tid=task_id: self.progress.remove_task(tid)).start()
                except Exception as e:
                    self.console.print(f"[red]Error: {e}")
    
    def phase3_sync_output(self):
        """Phase 3: Sync from cache to output sequentially with rate limiting."""
        self.progress.set_phase('uploading')
        
        # Find files to sync and count already synced
        files_to_sync = []
        already_synced_count = 0
        already_synced_size = 0
        
        for file_hash, cache_path in self.cache_map.items():
            try:
                size = os.path.getsize(cache_path)
                if file_hash in self.output_hashes:
                    already_synced_count += 1
                    already_synced_size += size
                else:
                    files_to_sync.append((file_hash, cache_path, size))
            except (OSError, IOError):
                continue
        
        # Set total including already synced files
        total_count = len(files_to_sync) + already_synced_count
        total_size = sum(f[2] for f in files_to_sync) + already_synced_size
        self.progress.set_total(total_count, total_size)
        
        # Start with already synced files counted
        self.progress.stats['uploaded_count'] = already_synced_count
        self.progress.stats['uploaded_size'] = already_synced_size
        
        if not files_to_sync:
            return
        
        # Sync files one by one
        for i, (file_hash, cache_path, size) in enumerate(files_to_sync):
            task_id = f"upload_{i}"
            
            # Determine output path
            cache_parts = Path(cache_path).relative_to(self.config['cache']['path']).parts
            if cache_parts:
                output_path = Path(self.config['output']['path']) / Path(*cache_parts)
                display_name = "/".join(cache_parts)
            else:
                output_path = Path(self.config['output']['path']) / Path(cache_path).name
                display_name = Path(cache_path).name
            
            self.progress.add_task(task_id, display_name, size, status="uploading")
            
            if self.copy_file_with_progress(cache_path, str(output_path), task_id, rate_limited=True):
                self.progress.complete_task(task_id)
                self.output_hashes.add(file_hash)
            
            self.progress.remove_task(task_id)
    
    def get_chunk_size(self, rate_limited: bool) -> int:
        """Get appropriate chunk size based on transfer type."""
        if rate_limited:
            # Smaller chunks for rate-limited transfers
            rate_limit = self.config['transfer'].get('rate_limit_mbps', 50)
            if rate_limit < 0.5:
                return 16 * 1024    # 16KB for very slow (< 0.5 MB/s)
            elif rate_limit < 1:
                return 32 * 1024    # 32KB for slow (< 1 MB/s)
            elif rate_limit < 10:
                return 128 * 1024   # 128KB for moderate (< 10 MB/s)
            else:
                return 256 * 1024   # 256KB for faster
        else:
            # Use config chunk size or default 1MB for cache transfers
            return self.config['transfer'].get('chunk_size', 1024 * 1024)
    
    def copy_file_with_progress(self, src: str, dst: str, task_id: str, 
                               rate_limited: bool = False) -> bool:
        """Copy a file with progress tracking."""
        try:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            
            size = os.path.getsize(src)
            bytes_copied = 0
            start_time = time.time()
            
            # Rate limiter setup if needed
            if rate_limited:
                rate_limit_mbps = self.config['transfer'].get('rate_limit_mbps', 50)
                rate_limit_bps = rate_limit_mbps * 1024 * 1024
            
            chunk_size = self.get_chunk_size(rate_limited)
            
            with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
                while chunk := fsrc.read(chunk_size):
                    fdst.write(chunk)
                    bytes_copied += len(chunk)
                    
                    # Rate limiting
                    if rate_limited:
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            current_rate = bytes_copied / elapsed
                            if current_rate > rate_limit_bps:
                                sleep_time = (bytes_copied / rate_limit_bps) - elapsed
                                if sleep_time > 0:
                                    time.sleep(sleep_time)
                    
                    self.progress.update_task(task_id, bytes_copied)
            
            # Copy metadata
            shutil.copystat(src, dst)
            
            # Copy hash file if it exists
            src_hash_file = self._get_hash_file_path(src)
            if os.path.exists(src_hash_file):
                dst_hash_file = self._get_hash_file_path(dst)
                shutil.copy2(src_hash_file, dst_hash_file)
            
            return True
            
        except (OSError, IOError) as e:
            self.console.print(f"[red]Error copying {src}: {e}")
            return False
    
    def cleanup_cache(self):
        """Clean up cache directory after successful sync."""
        self.progress.set_phase('cleaning')
        cache_path = self.config['cache']['path']
        
        if not os.path.exists(cache_path):
            return
        
        self.console.print("\n[yellow]Cleaning up cache directory...")
        
        try:
            # Remove all files and subdirectories in cache
            for root, dirs, files in os.walk(cache_path, topdown=False):
                for name in files:
                    file_path = os.path.join(root, name)
                    try:
                        os.remove(file_path)
                    except OSError as e:
                        self.console.print(f"[red]Error removing {file_path}: {e}")
                
                for name in dirs:
                    dir_path = os.path.join(root, name)
                    try:
                        os.rmdir(dir_path)
                    except OSError as e:
                        self.console.print(f"[red]Error removing directory {dir_path}: {e}")
            
            # Try to remove the cache directory itself
            try:
                os.rmdir(cache_path)
                self.console.print("[green]Cache directory cleaned up successfully")
            except OSError:
                # Directory might not be empty if some files couldn't be deleted
                self.console.print("[yellow]Cache directory not fully cleaned")
        
        except Exception as e:
            self.console.print(f"[red]Error during cache cleanup: {e}")
    
    def run(self):
        """Run the complete sync process."""
        with Live(self.progress.create_display(), refresh_per_second=10, console=self.console) as live:
            def update_display():
                while True:
                    live.update(self.progress.create_display())
                    time.sleep(0.1)
            
            # Start display update thread
            display_thread = threading.Thread(target=update_display, daemon=True)
            display_thread.start()
            
            # Phase 1: Calculate hashes
            self.phase1_hash_directories()
            
            # Phase 2: Cache all files
            self.phase2_cache_all()
            
            # Phase 3: Sync to output
            self.phase3_sync_output()
        
        # Clean up cache after successful sync
        self.cleanup_cache()
        
        self.console.print("\n[bold green]Sync complete!")
    
    def sync_specific_files(self, files_to_sync: List[Tuple[str, str]]):
        """Sync specific files that have changed."""
        if not files_to_sync:
            return
        
        # Phase 1: Hash output directory if needed
        if not self.output_hashes:
            self.phase1_hash_directories()
        
        # Phase 2: Cache specific files
        self.progress.set_phase('caching')
        files_to_cache = []
        
        for file_path, input_name in files_to_sync:
            if not os.path.exists(file_path):
                continue
            
            try:
                size = os.path.getsize(file_path)
                file_hash = self._calculate_hash(file_path)
                
                if not file_hash:
                    continue
                
                # Skip if already in output
                if file_hash in self.output_hashes:
                    continue
                
                # Determine cache path
                base_path = Path(self.config['inputs'][input_name]['path'])
                relative_path = Path(file_path).relative_to(base_path)
                cache_path = Path(self.config['cache']['path']) / input_name / relative_path
                
                files_to_cache.append((file_path, str(cache_path), size, input_name, file_hash))
            except Exception as e:
                self.console.print(f"[red]Error processing {file_path}: {e}")
        
        if files_to_cache:
            # Set total for progress
            total_size = sum(f[2] for f in files_to_cache)
            self.progress.set_total(len(files_to_cache), total_size)
            
            # Process files
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {}
                
                for i, (src, dst, size, input_name, file_hash) in enumerate(files_to_cache):
                    task_id = f"cache_{i}"
                    relative = Path(src).relative_to(self.config['inputs'][input_name]['path'])
                    display_name = f"{input_name}/{relative}"
                    
                    self.progress.add_task(task_id, display_name, size, status="caching")
                    
                    future = executor.submit(
                        self.copy_file_with_progress, src, dst, task_id, False
                    )
                    futures[future] = (task_id, file_hash, dst)
                
                for future in as_completed(futures):
                    task_id, file_hash, cache_path = futures[future]
                    try:
                        if future.result():
                            self.progress.complete_task(task_id)
                            self.cache_map[file_hash] = cache_path
                            threading.Timer(3.0, lambda tid=task_id: self.progress.remove_task(tid)).start()
                    except Exception as e:
                        self.console.print(f"[red]Error: {e}")
        
        # Phase 3: Sync to output
        self.phase3_sync_output()
    
    def watch(self):
        """Watch input directories for changes and sync automatically."""
        self.console.print("[bold cyan]Starting watch mode...")
        self.console.print("[yellow]Press Ctrl+C to stop watching")
        
        # Initialize state
        self.phase1_hash_directories()
        
        # Set up file system observers
        observer = Observer()
        event_handler = SyncEventHandler(self)
        
        # Schedule observers for each input directory
        for input_name, input_config in self.config['inputs'].items():
            input_path = input_config['path']
            if os.path.exists(input_path):
                observer.schedule(event_handler, input_path, recursive=True)
                self.console.print(f"[green]Watching: {input_path}")
            else:
                self.console.print(f"[yellow]Warning: {input_path} does not exist")
        
        # Start observer
        observer.start()
        
        # Main watch loop
        try:
            with Live(self.progress.create_display(), refresh_per_second=10, console=self.console) as live:
                def update_display():
                    while True:
                        live.update(self.progress.create_display())
                        time.sleep(0.1)
                
                # Start display update thread
                display_thread = threading.Thread(target=update_display, daemon=True)
                display_thread.start()
                
                # Set monitoring phase
                self.progress.set_phase('monitoring')
                
                while True:
                    # Check for pending files every second
                    time.sleep(1.0)
                    
                    files_to_process = event_handler.get_pending_files()
                    if files_to_process:
                        self.console.print(f"\n[cyan]Processing {len(files_to_process)} changed files...")
                        self.sync_specific_files(files_to_process)
                        
                        # Clean up cache after sync
                        self.cleanup_cache()
                        
                        # Return to monitoring phase
                        self.progress.set_phase('monitoring')
                        self.console.print("[green]Ready for more changes...")
        
        except KeyboardInterrupt:
            self.console.print("\n[yellow]Stopping watch mode...")
            observer.stop()
            observer.join()
            self.console.print("[green]Watch mode stopped")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Homely Copier - Sync files with caching and rate limiting')
    parser.add_argument('--watch', action='store_true', help='Watch input directories for changes')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path (default: config.yaml)')
    
    args = parser.parse_args()
    
    try:
        sync = SimpleSync(args.config)
        
        if args.watch:
            sync.watch()
        else:
            sync.run()
    except KeyboardInterrupt:
        print("\n\nSync interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()