#!/usr/bin/env python3
"""
Migration Performance Monitor
Real-time monitoring and optimization for large-scale data migration

Features:
- Real-time performance metrics
- Automatic batch size optimization
- Connection pool monitoring
- Memory usage tracking
- Progress estimation
- Performance alerts
"""

import time
import psutil
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import json
import threading
from collections import deque
import statistics

@dataclass
class PerformanceMetrics:
    """Performance metrics for migration monitoring"""
    timestamp: datetime = field(default_factory=datetime.now)
    
    # Throughput metrics
    records_per_second: float = 0.0
    bytes_per_second: float = 0.0
    batch_duration_ms: float = 0.0
    
    # Resource metrics
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    memory_mb: float = 0.0
    
    # Database metrics
    active_connections: int = 0
    connection_wait_time_ms: float = 0.0
    query_duration_ms: float = 0.0
    
    # Error metrics
    error_rate: float = 0.0
    retry_count: int = 0
    
    # Progress metrics
    progress_percent: float = 0.0
    eta_minutes: Optional[float] = None

class PerformanceMonitor:
    """Real-time performance monitoring for migration pipeline"""
    
    def __init__(self, 
                 window_size: int = 100,
                 alert_thresholds: Optional[Dict] = None,
                 log_interval: int = 30):
        """
        Initialize performance monitor
        
        Args:
            window_size: Number of recent metrics to keep for rolling averages
            alert_thresholds: Thresholds for performance alerts
            log_interval: Interval in seconds for logging metrics
        """
        self.window_size = window_size
        self.log_interval = log_interval
        
        # Default alert thresholds
        self.alert_thresholds = alert_thresholds or {
            'cpu_percent': 90.0,
            'memory_percent': 85.0,
            'error_rate': 0.05,  # 5%
            'connection_wait_time_ms': 1000.0,
            'records_per_second_min': 100.0
        }
        
        # Metrics storage
        self.metrics_history: deque = deque(maxlen=window_size)
        self.alerts_history: List[Dict] = []
        
        # Monitoring state
        self.monitoring = False
        self.start_time = None
        self.total_records_processed = 0
        self.total_records_target = 0
        
        # Threading
        self.monitor_thread = None
        self.stop_event = threading.Event()
        
        # Logger
        self.logger = logging.getLogger('performance_monitor')
        
        # Optimization state
        self.optimal_batch_size = None
        self.batch_size_history = []
        self.performance_samples = deque(maxlen=20)  # For batch size optimization
    
    def start_monitoring(self, total_records: int = 0):
        """Start performance monitoring"""
        self.monitoring = True
        self.start_time = datetime.now()
        self.total_records_target = total_records
        self.stop_event.clear()
        
        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitor_thread.start()
        
        self.logger.info("Performance monitoring started")
    
    def stop_monitoring(self):
        """Stop performance monitoring"""
        self.monitoring = False
        self.stop_event.set()
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)
        
        self.logger.info("Performance monitoring stopped")
    
    def record_batch_metrics(self, 
                           records_processed: int,
                           batch_size: int,
                           duration_ms: float,
                           bytes_processed: int = 0,
                           errors: int = 0,
                           retries: int = 0,
                           connection_wait_ms: float = 0,
                           query_duration_ms: float = 0):
        """Record metrics for a completed batch"""
        
        self.total_records_processed += records_processed
        
        # Calculate rates
        records_per_second = records_processed / (duration_ms / 1000.0) if duration_ms > 0 else 0
        bytes_per_second = bytes_processed / (duration_ms / 1000.0) if duration_ms > 0 and bytes_processed > 0 else 0
        error_rate = errors / records_processed if records_processed > 0 else 0
        
        # Calculate progress
        progress_percent = (self.total_records_processed / self.total_records_target * 100) if self.total_records_target > 0 else 0
        
        # Calculate ETA
        eta_minutes = None
        if self.start_time and records_per_second > 0 and self.total_records_target > 0:
            remaining_records = self.total_records_target - self.total_records_processed
            eta_seconds = remaining_records / records_per_second
            eta_minutes = eta_seconds / 60.0
        
        # Get system metrics
        cpu_percent = psutil.cpu_percent()
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_mb = memory.used / 1024 / 1024
        
        # Create metrics record
        metrics = PerformanceMetrics(
            records_per_second=records_per_second,
            bytes_per_second=bytes_per_second,
            batch_duration_ms=duration_ms,
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_mb=memory_mb,
            connection_wait_time_ms=connection_wait_ms,
            query_duration_ms=query_duration_ms,
            error_rate=error_rate,
            retry_count=retries,
            progress_percent=progress_percent,
            eta_minutes=eta_minutes
        )
        
        # Store metrics
        self.metrics_history.append(metrics)
        
        # Store performance sample for batch size optimization
        self.performance_samples.append({
            'batch_size': batch_size,
            'records_per_second': records_per_second,
            'duration_ms': duration_ms,
            'cpu_percent': cpu_percent,
            'memory_percent': memory_percent
        })
        
        # Check for alerts
        self._check_alerts(metrics)
        
        # Log metrics periodically
        if len(self.metrics_history) % (self.log_interval * 2) == 0:  # Approximate interval
            self._log_current_metrics(metrics)
    
    def get_current_metrics(self) -> Optional[PerformanceMetrics]:
        """Get the most recent performance metrics"""
        return self.metrics_history[-1] if self.metrics_history else None
    
    def get_average_metrics(self, window: int = None) -> Optional[PerformanceMetrics]:
        """Get average metrics over a window"""
        if not self.metrics_history:
            return None
        
        window = min(window or self.window_size, len(self.metrics_history))
        recent_metrics = list(self.metrics_history)[-window:]
        
        # Calculate averages
        avg_metrics = PerformanceMetrics(
            records_per_second=statistics.mean(m.records_per_second for m in recent_metrics),
            bytes_per_second=statistics.mean(m.bytes_per_second for m in recent_metrics),
            batch_duration_ms=statistics.mean(m.batch_duration_ms for m in recent_metrics),
            cpu_percent=statistics.mean(m.cpu_percent for m in recent_metrics),
            memory_percent=statistics.mean(m.memory_percent for m in recent_metrics),
            memory_mb=statistics.mean(m.memory_mb for m in recent_metrics),
            connection_wait_time_ms=statistics.mean(m.connection_wait_time_ms for m in recent_metrics),
            query_duration_ms=statistics.mean(m.query_duration_ms for m in recent_metrics),
            error_rate=statistics.mean(m.error_rate for m in recent_metrics),
            progress_percent=recent_metrics[-1].progress_percent,  # Use latest progress
            eta_minutes=recent_metrics[-1].eta_minutes  # Use latest ETA
        )
        
        return avg_metrics
    
    def get_optimal_batch_size(self, current_batch_size: int) -> int:
        """Calculate optimal batch size based on performance history"""
        if len(self.performance_samples) < 5:
            return current_batch_size
        
        # Analyze performance samples
        samples = list(self.performance_samples)[-10:]  # Last 10 samples
        
        # Find the batch size with best records/second while staying under resource limits
        best_performance = 0
        best_batch_size = current_batch_size
        
        for sample in samples:
            # Skip samples with high resource usage or errors
            if sample['cpu_percent'] > 80 or sample['memory_percent'] > 80:
                continue
            
            performance_score = sample['records_per_second']
            if performance_score > best_performance:
                best_performance = performance_score
                best_batch_size = sample['batch_size']
        
        # Suggest adjustments
        current_avg_rps = statistics.mean(s['records_per_second'] for s in samples[-3:])
        current_avg_cpu = statistics.mean(s['cpu_percent'] for s in samples[-3:])
        current_avg_memory = statistics.mean(s['memory_percent'] for s in samples[-3:])
        
        # Conservative optimization
        if current_avg_cpu < 50 and current_avg_memory < 60:
            # System has capacity, try larger batch size
            suggested = min(current_batch_size * 1.2, current_batch_size + 5000)
        elif current_avg_cpu > 80 or current_avg_memory > 80:
            # System under stress, reduce batch size
            suggested = max(current_batch_size * 0.8, 1000)
        else:
            # System balanced, keep current size
            suggested = current_batch_size
        
        return int(suggested)
    
    def _monitoring_loop(self):
        """Background monitoring loop"""
        while not self.stop_event.is_set():
            try:
                # Log metrics every interval
                self.stop_event.wait(self.log_interval)
                
                if not self.stop_event.is_set() and self.metrics_history:
                    current_metrics = self.get_current_metrics()
                    avg_metrics = self.get_average_metrics(10)  # 10-sample average
                    
                    self.logger.info(
                        f"Migration Progress: {current_metrics.progress_percent:.1f}% | "
                        f"Rate: {avg_metrics.records_per_second:.1f} rec/s | "
                        f"CPU: {avg_metrics.cpu_percent:.1f}% | "
                        f"Memory: {avg_metrics.memory_percent:.1f}% | "
                        f"ETA: {current_metrics.eta_minutes:.1f}min" if current_metrics.eta_minutes else "ETA: Unknown"
                    )
            
            except Exception as e:
                self.logger.error(f"Monitoring loop error: {e}")
    
    def _check_alerts(self, metrics: PerformanceMetrics):
        """Check for performance alerts"""
        alerts = []
        
        # CPU alert
        if metrics.cpu_percent > self.alert_thresholds['cpu_percent']:
            alerts.append({
                'type': 'cpu_high',
                'value': metrics.cpu_percent,
                'threshold': self.alert_thresholds['cpu_percent'],
                'message': f"High CPU usage: {metrics.cpu_percent:.1f}%"
            })
        
        # Memory alert
        if metrics.memory_percent > self.alert_thresholds['memory_percent']:
            alerts.append({
                'type': 'memory_high',
                'value': metrics.memory_percent,
                'threshold': self.alert_thresholds['memory_percent'],
                'message': f"High memory usage: {metrics.memory_percent:.1f}%"
            })
        
        # Error rate alert
        if metrics.error_rate > self.alert_thresholds['error_rate']:
            alerts.append({
                'type': 'error_rate_high',
                'value': metrics.error_rate,
                'threshold': self.alert_thresholds['error_rate'],
                'message': f"High error rate: {metrics.error_rate:.2%}"
            })
        
        # Connection wait time alert
        if metrics.connection_wait_time_ms > self.alert_thresholds['connection_wait_time_ms']:
            alerts.append({
                'type': 'connection_wait_high',
                'value': metrics.connection_wait_time_ms,
                'threshold': self.alert_thresholds['connection_wait_time_ms'],
                'message': f"High connection wait time: {metrics.connection_wait_time_ms:.1f}ms"
            })
        
        # Low performance alert
        if len(self.metrics_history) > 5:
            avg_rps = statistics.mean(m.records_per_second for m in list(self.metrics_history)[-5:])
            if avg_rps < self.alert_thresholds['records_per_second_min']:
                alerts.append({
                    'type': 'performance_low',
                    'value': avg_rps,
                    'threshold': self.alert_thresholds['records_per_second_min'],
                    'message': f"Low processing rate: {avg_rps:.1f} records/s"
                })
        
        # Log alerts
        for alert in alerts:
            self.alerts_history.append({
                **alert,
                'timestamp': datetime.now()
            })
            self.logger.warning(f"ALERT: {alert['message']}")
    
    def _log_current_metrics(self, metrics: PerformanceMetrics):
        """Log current performance metrics"""
        self.logger.info(
            f"Performance: {metrics.records_per_second:.1f} rec/s | "
            f"Batch: {metrics.batch_duration_ms:.1f}ms | "
            f"CPU: {metrics.cpu_percent:.1f}% | "
            f"Memory: {metrics.memory_percent:.1f}% ({metrics.memory_mb:.1f}MB) | "
            f"Progress: {metrics.progress_percent:.1f}% | "
            f"Errors: {metrics.error_rate:.2%}"
        )
    
    def get_performance_report(self) -> Dict:
        """Generate comprehensive performance report"""
        if not self.metrics_history:
            return {"status": "no_data"}
        
        current = self.get_current_metrics()
        average = self.get_average_metrics()
        
        # Calculate total duration
        duration = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        # Performance summary
        report = {
            "summary": {
                "total_duration_seconds": duration,
                "total_records_processed": self.total_records_processed,
                "average_records_per_second": average.records_per_second if average else 0,
                "peak_records_per_second": max(m.records_per_second for m in self.metrics_history),
                "current_progress_percent": current.progress_percent if current else 0,
                "estimated_completion_minutes": current.eta_minutes if current else None
            },
            "current_metrics": {
                "records_per_second": current.records_per_second if current else 0,
                "cpu_percent": current.cpu_percent if current else 0,
                "memory_percent": current.memory_percent if current else 0,
                "memory_mb": current.memory_mb if current else 0,
                "error_rate": current.error_rate if current else 0
            },
            "averages": {
                "records_per_second": average.records_per_second if average else 0,
                "batch_duration_ms": average.batch_duration_ms if average else 0,
                "cpu_percent": average.cpu_percent if average else 0,
                "memory_percent": average.memory_percent if average else 0,
                "connection_wait_time_ms": average.connection_wait_time_ms if average else 0,
                "query_duration_ms": average.query_duration_ms if average else 0
            },
            "alerts": {
                "total_alerts": len(self.alerts_history),
                "recent_alerts": [a for a in self.alerts_history if (datetime.now() - a['timestamp']).seconds < 300]  # Last 5 minutes
            },
            "optimization": {
                "optimal_batch_size": self.get_optimal_batch_size(10000),  # Use 10k as reference
                "performance_samples": len(self.performance_samples)
            }
        }
        
        return report
    
    def export_metrics(self, filepath: str):
        """Export metrics to JSON file"""
        data = {
            "export_timestamp": datetime.now().isoformat(),
            "performance_report": self.get_performance_report(),
            "metrics_history": [
                {
                    "timestamp": m.timestamp.isoformat(),
                    "records_per_second": m.records_per_second,
                    "batch_duration_ms": m.batch_duration_ms,
                    "cpu_percent": m.cpu_percent,
                    "memory_percent": m.memory_percent,
                    "memory_mb": m.memory_mb,
                    "connection_wait_time_ms": m.connection_wait_time_ms,
                    "query_duration_ms": m.query_duration_ms,
                    "error_rate": m.error_rate,
                    "progress_percent": m.progress_percent,
                    "eta_minutes": m.eta_minutes
                }
                for m in self.metrics_history
            ],
            "alerts_history": self.alerts_history
        }
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        
        self.logger.info(f"Metrics exported to {filepath}")

class DatabaseConnectionMonitor:
    """Monitor database connection pool performance"""
    
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool
        self.connection_times = deque(maxlen=100)
        self.active_connections = 0
        self.logger = logging.getLogger('db_connection_monitor')
    
    def time_connection_acquisition(self):
        """Context manager to time connection acquisition"""
        class ConnectionTimer:
            def __init__(self, monitor):
                self.monitor = monitor
                self.start_time = None
                self.conn = None
            
            def __enter__(self):
                self.start_time = time.time()
                self.conn = self.monitor.connection_pool.getconn()
                acquisition_time = (time.time() - self.start_time) * 1000  # ms
                self.monitor.connection_times.append(acquisition_time)
                self.monitor.active_connections += 1
                return self.conn
            
            def __exit__(self, exc_type, exc_val, exc_tb):
                if self.conn:
                    self.monitor.connection_pool.putconn(self.conn)
                    self.monitor.active_connections -= 1
        
        return ConnectionTimer(self)
    
    def get_connection_stats(self) -> Dict:
        """Get connection pool statistics"""
        if not self.connection_times:
            return {"status": "no_data"}
        
        return {
            "active_connections": self.active_connections,
            "average_acquisition_time_ms": statistics.mean(self.connection_times),
            "max_acquisition_time_ms": max(self.connection_times),
            "min_acquisition_time_ms": min(self.connection_times),
            "recent_acquisition_times": list(self.connection_times)[-10:]  # Last 10
        }

# Example usage and integration with migration pipeline
if __name__ == "__main__":
    # Example usage
    monitor = PerformanceMonitor(
        window_size=100,
        alert_thresholds={
            'cpu_percent': 85.0,
            'memory_percent': 80.0,
            'error_rate': 0.03,
            'records_per_second_min': 500.0
        }
    )
    
    # Start monitoring
    monitor.start_monitoring(total_records=1000000)
    
    # Simulate batch processing
    import random
    for i in range(100):
        time.sleep(0.1)  # Simulate processing time
        
        # Record metrics for this batch
        monitor.record_batch_metrics(
            records_processed=random.randint(800, 1200),
            batch_size=1000,
            duration_ms=random.uniform(50, 150),
            bytes_processed=random.randint(50000, 150000),
            errors=random.randint(0, 5),
            connection_wait_ms=random.uniform(1, 20),
            query_duration_ms=random.uniform(30, 100)
        )
    
    # Stop monitoring
    monitor.stop_monitoring()
    
    # Generate report
    report = monitor.get_performance_report()
    print(json.dumps(report, indent=2, default=str))
    
    # Export metrics
    monitor.export_metrics("migration_metrics.json")


