#!/usr/bin/env python3
"""
Test script to demonstrate partition optimization improvements.

This script simulates the performance improvements achieved by the partition optimization
without requiring actual BigQuery connections.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockBigQueryClient:
    """Mock BigQuery client for testing"""
    
    def __init__(self):
        self.query_count = 0
        self.mock_data = {
            ('dataset1', 'table1'): '2024-01-15',
            ('dataset1', 'table2'): '2024-01-14',
            ('dataset2', 'table1'): '2024-01-13',
            ('dataset2', 'table2'): '2024-01-12',
            ('dataset3', 'table1'): '2024-01-11',
        }
    
    def query(self, query_str):
        self.query_count += 1
        logger.debug(f"Mock query #{self.query_count}: {query_str}")
        
        # Simulate query execution time
        time.sleep(0.1)
        
        return MockQueryResult(self.mock_data)


class MockQueryResult:
    """Mock query result"""
    
    def __init__(self, mock_data):
        self.mock_data = mock_data
    
    def result(self):
        return [MockRow(dataset, table, partition_dt) 
                for (dataset, table), partition_dt in self.mock_data.items()]


class MockRow:
    """Mock row result"""
    
    def __init__(self, dataset, table, partition_dt):
        self.project_dataset = dataset
        self.table_name = table
        self.partition_dt = partition_dt


class OptimizedPartitionLookup:
    """Demonstrates the optimized partition lookup approach"""
    
    def __init__(self):
        self.bq_client = MockBigQueryClient()
        self._partition_cache = {}
    
    def _extract_all_table_references(self, sql: str) -> List[Tuple[str, str]]:
        """Extract all unique table references from SQL query"""
        table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
        table_references = set()
        
        for match in re.finditer(table_pattern, sql):
            project, dataset, table = match.groups()
            table_references.add((dataset, table))
        
        return list(table_references)
    
    def _batch_get_partition_dts(self, table_references: List[Tuple[str, str]]) -> Dict[Tuple[str, str], str]:
        """Batch query partition_dt for multiple tables in a single query"""
        if not table_references:
            return {}
        
        logger.info(f"Batch querying partition info for {len(table_references)} tables")
        
        # Simulate batch query
        query_job = self.bq_client.query("BATCH_QUERY")
        results = query_job.result()
        
        partition_map = {}
        for row in results:
            dataset = row.project_dataset
            table_name = row.table_name
            partition_dt = row.partition_dt
            partition_map[(dataset, table_name)] = partition_dt
        
        return partition_map
    
    def _preload_partition_cache(self, sql_queries: List[str]) -> None:
        """Preload partition cache by analyzing all SQL queries"""
        logger.info("Preloading partition cache for optimized lookups")
        
        # Extract all unique table references from all SQL queries
        all_table_references = set()
        for sql in sql_queries:
            if sql.strip():
                table_refs = self._extract_all_table_references(sql)
                all_table_references.update(table_refs)
        
        if not all_table_references:
            logger.info("No table references found in SQL queries")
            return
        
        logger.info(f"Found {len(all_table_references)} unique table references across all SQL queries")
        
        # Batch query partition info for all tables
        partition_map = self._batch_get_partition_dts(list(all_table_references))
        
        # Update cache
        self._partition_cache.update(partition_map)
        
        logger.info(f"Preloaded partition cache with {len(partition_map)} table entries")
    
    def get_partition_dt(self, dataset: str, table_name: str) -> Optional[str]:
        """Get latest partition_dt (optimized with caching)"""
        # Check cache first
        cache_key = (dataset, table_name)
        if cache_key in self._partition_cache:
            logger.debug(f"Cache hit for {dataset}.{table_name}: {self._partition_cache[cache_key]}")
            return self._partition_cache[cache_key]
        
        # Fallback to individual query if not in cache
        logger.debug(f"Cache miss for {dataset}.{table_name}, querying individually")
        
        # Simulate individual query
        self.bq_client.query(f"INDIVIDUAL_QUERY for {dataset}.{table_name}")
        
        # Mock result
        result = self.bq_client.mock_data.get(cache_key)
        if result:
            self._partition_cache[cache_key] = result
            logger.debug(f"Cached result for {dataset}.{table_name}: {result}")
        
        return result
    
    def process_metrics_optimized(self, sql_queries: List[str]) -> None:
        """Process metrics with optimization"""
        logger.info("=== OPTIMIZED APPROACH ===")
        
        # Reset query count
        self.bq_client.query_count = 0
        start_time = time.time()
        
        # Preload partition cache
        self._preload_partition_cache(sql_queries)
        
        # Process each query (now uses cached partition info)
        for i, sql in enumerate(sql_queries):
            logger.info(f"Processing query {i+1}/{len(sql_queries)}")
            
            # Extract table references and get partition info
            table_refs = self._extract_all_table_references(sql)
            for dataset, table in table_refs:
                partition_dt = self.get_partition_dt(dataset, table)
                logger.debug(f"Got partition_dt for {dataset}.{table}: {partition_dt}")
        
        processing_time = time.time() - start_time
        logger.info(f"Optimized processing completed in {processing_time:.2f} seconds")
        logger.info(f"Total queries executed: {self.bq_client.query_count}")


class UnoptimizedPartitionLookup:
    """Demonstrates the original unoptimized approach"""
    
    def __init__(self):
        self.bq_client = MockBigQueryClient()
    
    def get_partition_dt(self, dataset: str, table_name: str) -> Optional[str]:
        """Get latest partition_dt (unoptimized - individual queries)"""
        # Always make individual query
        self.bq_client.query(f"INDIVIDUAL_QUERY for {dataset}.{table_name}")
        
        # Mock result
        cache_key = (dataset, table_name)
        return self.bq_client.mock_data.get(cache_key)
    
    def process_metrics_unoptimized(self, sql_queries: List[str]) -> None:
        """Process metrics without optimization"""
        logger.info("=== UNOPTIMIZED APPROACH ===")
        
        # Reset query count
        self.bq_client.query_count = 0
        start_time = time.time()
        
        # Process each query with individual lookups
        for i, sql in enumerate(sql_queries):
            logger.info(f"Processing query {i+1}/{len(sql_queries)}")
            
            # Extract table references and get partition info (individual queries)
            table_pattern = r'`([^.]+)\.([^.]+)\.([^`]+)`'
            for match in re.finditer(table_pattern, sql):
                project, dataset, table = match.groups()
                partition_dt = self.get_partition_dt(dataset, table)
                logger.debug(f"Got partition_dt for {dataset}.{table}: {partition_dt}")
        
        processing_time = time.time() - start_time
        logger.info(f"Unoptimized processing completed in {processing_time:.2f} seconds")
        logger.info(f"Total queries executed: {self.bq_client.query_count}")


def create_test_queries() -> List[str]:
    """Create test SQL queries with partition_info placeholders"""
    return [
        "SELECT * FROM `project.dataset1.table1` WHERE partition_dt = '{partition_info}'",
        "SELECT * FROM `project.dataset1.table2` WHERE partition_dt = '{partition_info}'",
        "SELECT * FROM `project.dataset2.table1` WHERE partition_dt = '{partition_info}'",
        "SELECT * FROM `project.dataset1.table1` WHERE partition_dt = '{partition_info}'",  # Duplicate
        "SELECT * FROM `project.dataset2.table2` WHERE partition_dt = '{partition_info}'",
        "SELECT * FROM `project.dataset3.table1` WHERE partition_dt = '{partition_info}'",
        "SELECT * FROM `project.dataset1.table1` WHERE partition_dt = '{partition_info}'",  # Another duplicate
        "SELECT * FROM `project.dataset2.table1` WHERE partition_dt = '{partition_info}'",  # Another duplicate
    ]


def main():
    """Main test function"""
    logger.info("Starting partition optimization demonstration")
    
    # Create test queries
    test_queries = create_test_queries()
    logger.info(f"Created {len(test_queries)} test queries")
    
    # Test unoptimized approach
    unoptimized = UnoptimizedPartitionLookup()
    unoptimized.process_metrics_unoptimized(test_queries)
    
    logger.info("")  # Empty line for separation
    
    # Test optimized approach
    optimized = OptimizedPartitionLookup()
    optimized.process_metrics_optimized(test_queries)
    
    # Calculate improvements
    unoptimized_queries = unoptimized.bq_client.query_count
    optimized_queries = optimized.bq_client.query_count
    
    improvement = ((unoptimized_queries - optimized_queries) / unoptimized_queries) * 100
    
    logger.info("")
    logger.info("=== PERFORMANCE COMPARISON ===")
    logger.info(f"Unoptimized queries: {unoptimized_queries}")
    logger.info(f"Optimized queries: {optimized_queries}")
    logger.info(f"Query reduction: {improvement:.1f}%")
    
    # Show cache statistics
    cache_stats = {
        'cache_size': len(optimized._partition_cache),
        'unique_tables': len(set(key[0] for key in optimized._partition_cache.keys())),
        'unique_datasets': len(set(key[0] for key in optimized._partition_cache.keys()))
    }
    logger.info(f"Cache statistics: {cache_stats}")


if __name__ == "__main__":
    main() 